"""
prefect_flow.py
-------------------
Student project submission ETL pipeline:

- Extract NYC Taxi data (Parquet)
- Extract Taxi Zone lookup (CSV)
- Extract Weather data (JSON)
- Transform & enrich data using PySpark
- Stage transformed data as Parquet
- Load into DuckDB (analytical database)
- Inspect final table
- Orchestrated with Prefect (deployment-ready, schedulable)

This version is Windows-safe and production-like.
"""

import os
import shutil
import sys
import urllib.request
from datetime import timedelta, datetime

from prefect import flow, task
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, avg, coalesce, lit, explode,
    to_timestamp, to_date, hour, dayofweek,
    unix_timestamp, lower, round
)
import duckdb

# ==========================================================================
# CONFIGURATION
# ==========================================================================
BASE_DIR = os.getcwd()
DATA_DIR = os.path.join(BASE_DIR, "data")

RAW_DIR = os.path.join(DATA_DIR, "raw")
STAGING_DIR = os.path.join(DATA_DIR, "staging_parquet")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")

DUCKDB_PATH = os.path.join(PROCESSED_DIR, "taxi_weather.duckdb")
HADOOP_HOME = os.path.join(DATA_DIR, "hadoop_home")

TAXI_PARQUET = os.path.join(RAW_DIR, "taxi_parquet")
ZONE_CSV = os.path.join(RAW_DIR, "taxi_zone_lookup.csv")
WEATHER_JSON = os.path.join(RAW_DIR, "weather", "weather_nyc.json")

os.makedirs(STAGING_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(os.path.dirname(WEATHER_JSON), exist_ok=True)

# ============================================================================
# SPARK SETUP (WINDOWS SAFE)
# ===========================================================================
def setup_windows_hadoop():
    if sys.platform.startswith("win"):
        bin_dir = os.path.join(HADOOP_HOME, "bin")
        os.makedirs(bin_dir, exist_ok=True)

        winutils = os.path.join(bin_dir, "winutils.exe")
        hadoop_dll = os.path.join(bin_dir, "hadoop.dll")

        if not os.path.exists(winutils):
            urllib.request.urlretrieve(
                "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe",
                winutils
            )
        if not os.path.exists(hadoop_dll):
            urllib.request.urlretrieve(
                "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/hadoop.dll",
                hadoop_dll
            )

        os.environ["HADOOP_HOME"] = HADOOP_HOME
        os.environ["PATH"] += os.pathsep + bin_dir


def get_spark():
    setup_windows_hadoop()
    return (
        SparkSession.builder
        .appName("NYC Taxi ETL")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

# =============================================================================
# PREFECT CACHE (DAILY)
# =============================================================================
def cache_daily_key(context, parameters):
    return str(datetime.today().date())

# ============================================================================
# TASK: TRANSFORM DATA (PYSPARK)
# ============================================================================
@task(
    name="Transform Taxi & Weather Data",
    retries=2,
    retry_delay_seconds=5,
    cache_key_fn=cache_daily_key,
    cache_expiration=timedelta(days=1),
)
def transform_data():
    spark = get_spark()

    # ----------------------------------------------------------
    # LOAD DATA
    # ---------------------------------------------------------
    df_taxi = spark.read.parquet(TAXI_PARQUET)
    df_zone = spark.read.option("header", True).csv(ZONE_CSV)
    df_weather_raw = spark.read.option("multiline", "true").json(WEATHER_JSON)

    # ------------------------------------------------
    # CLEAN TAXI DATA
    # ---------------------------------------------
    df_taxi_clean = (
        df_taxi
        .withColumn("pickup_datetime", to_timestamp("tpep_pickup_datetime"))
        .withColumn("dropoff_datetime", to_timestamp("tpep_dropoff_datetime"))
        .filter(
            (col("trip_distance") > 0) &
            (col("fare_amount") > 0) &
            col("pickup_datetime").isNotNull() &
            col("dropoff_datetime").isNotNull()
        )
        .fillna({
            "passenger_count": 1,
            "tip_amount": 0.0,
            "congestion_surcharge": 0.0
        })
        .withColumn(
            "trip_duration_min",
            round(
                (unix_timestamp("dropoff_datetime") -
                 unix_timestamp("pickup_datetime")) / 60, 2
            )
        )
        .filter((col("trip_duration_min") > 0) & (col("trip_duration_min") <= 1440))
    )

    # -------------------------
    # GEO ENRICHMENT
    # -------------------------
    df_zone = (
        df_zone
        .withColumnRenamed("LocationID", "PULocationID")
        .withColumnRenamed("Borough", "PU_Borough")
    )

    df_taxi_geo = (
        df_taxi_clean
        .join(df_zone, "PULocationID", "left")
        .withColumn("pickup_date", to_date("pickup_datetime"))
        .withColumn("pickup_hour", hour("pickup_datetime"))
        .withColumn("pickup_dayofweek", dayofweek("pickup_datetime"))
        .withColumn("is_weekend", dayofweek("pickup_datetime").isin([1, 7]))
    )

    # -------------------------
    # WEATHER ENRICHMENT
    # -------------------------
    df_weather = (
        df_weather_raw
        .select(explode("days").alias("day"))
        .select(
            to_date(to_timestamp(col("day.datetime"))).alias("weather_date"),
            explode(col("day.hours")).alias("hour")
        )
        .select(
            col("weather_date"),
            hour(to_timestamp(col("hour.datetime"))).alias("weather_hour"),
            col("hour.temp").alias("temperature"),
            col("hour.conditions")
        )
        .withColumn(
            "weather_category",
            when(lower(col("conditions")).like("%rain%"), "Rain")
            .when(lower(col("conditions")).like("%snow%"), "Snow")
            .when(lower(col("conditions")).like("%cloud%"), "Cloudy")
            .otherwise("Clear")
        )
        .withColumn("weather_dayofweek", dayofweek("weather_date"))
    )

    global_avg_temp = (
        df_weather.agg(avg("temperature")).collect()[0][0] or 10.0
    )

    weather_patterns = (
        df_weather
        .groupBy("weather_dayofweek", "weather_hour")
        .agg(avg("temperature").alias("avg_temp_dow_hour"))
        .withColumnRenamed("weather_dayofweek", "pattern_dow")
        .withColumnRenamed("weather_hour", "pattern_hour")
    )

    # -------------------------
    # FINAL JOIN
    # -------------------------
    df_final = (
        df_taxi_geo
        .join(
            df_weather,
            (df_taxi_geo.pickup_date == df_weather.weather_date) &
            (df_taxi_geo.pickup_hour == df_weather.weather_hour),
            "left"
        )
        .join(
            weather_patterns,
            (df_taxi_geo.pickup_dayofweek == weather_patterns.pattern_dow) &
            (df_taxi_geo.pickup_hour == weather_patterns.pattern_hour),
            "left"
        )
        .withColumn(
            "temperature",
            coalesce(col("temperature"), col("avg_temp_dow_hour"), lit(global_avg_temp))
        )
        .withColumn("weather_date", coalesce(col("weather_date"), col("pickup_date")))
        .withColumn("weather_hour", coalesce(col("weather_hour"), col("pickup_hour")))
        .drop(
            "avg_temp_dow_hour",
            "pattern_dow",
            "pattern_hour",
            "weather_dayofweek",
            "pickup_dayofweek"
        )
    )

    # -------------------------
    # WRITE STAGING PARQUET
    # -------------------------
    if os.path.exists(STAGING_DIR):
        shutil.rmtree(STAGING_DIR)

    df_final.write.mode("overwrite").parquet(STAGING_DIR)
    spark.stop()

    print(f"Transformation complete. Parquet saved at {STAGING_DIR}")
    return STAGING_DIR


# =====================================================
# TASK: LOAD + INSPECT DUCKDB (WINDOWS SAFE)
# =====================================================
@task(
    name="Load & Inspect DuckDB",
    retries=2,
    retry_delay_seconds=5,
    cache_key_fn=cache_daily_key,
    cache_expiration=timedelta(days=1),
)
def load_and_inspect_duckdb(parquet_path):
    if os.path.exists(DUCKDB_PATH):
        os.remove(DUCKDB_PATH)

    with duckdb.connect(DUCKDB_PATH) as con:
        parquet_glob = os.path.join(parquet_path, "*.parquet").replace("\\", "/")

        con.execute("""
            CREATE TABLE taxi_weather AS
            SELECT * FROM read_parquet(?)
        """, [parquet_glob])

        rows = con.execute("SELECT COUNT(*) FROM taxi_weather").fetchone()[0]
        cols = len(con.execute("PRAGMA table_info(taxi_weather)").fetchall())

        print("\nDuckDB Load Complete")
        print(f"Rows: {rows} | Columns: {cols}")
        print("Sample rows:")

        for row in con.execute("SELECT * FROM taxi_weather LIMIT 5").fetchall():
            print(row)

# =====================================================
# MAIN FLOW
# =====================================================
@flow(name="NYC Taxi ETL Pipeline", log_prints=True)
def main_flow():
    parquet_path = transform_data()
    load_and_inspect_duckdb(parquet_path)
    print("ETL flow completed successfully!")

# =====================================================
# ENTRY POINT (DEPLOYMENT-READY)
# =====================================================
if __name__ == "__main__":
    print(f"\nRunning ETL flow manually at {datetime.now()}")
    main_flow()

    print("\nFor scheduled execution using Prefect:")
    print("1️⃣ prefect deployment build prefect_flow.py:main_flow -n 'daily_nyc_taxi_etl' --cron '0 9 * * *'")
    print("2️⃣ prefect deployment apply main_flow-deployment.yaml")
    print("3️⃣ prefect deployment run 'daily_nyc_taxi_etl'")

