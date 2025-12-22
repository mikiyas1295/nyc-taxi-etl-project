deme

---

# 🚕📊 NYC Taxi & Weather Analytics ETL Pipeline

---

## 📌✨ Project Overview

This project designs and implements a **scalable, resilient ETL pipeline** that integrates large-scale **NYC taxi transportation data** with **real-time weather information**.

Using **⚡ Apache PySpark** for distributed data processing, **🦆 DuckDB** for high-performance analytics, and **🧭 Prefect** for orchestration, the pipeline extracts, transforms, and loads heterogeneous data sources into an **analytics-ready database**.

The final curated dataset is visualized using a **📊 Business Intelligence dashboard** (Power BI / Tableau / Looker) to support **data-driven decision-making**.

---

## 🎯📈 Business Problem

Urban transportation demand is influenced by **⏰ time**, **📍 location**, and **🌦️ weather conditions**.
This project answers the following key analytical questions:

* 🚕 How do taxi trips vary by **hour**, **weekday**, and **borough**?
* 🌧️ How does **weather** (rain, snow, temperature) affect **trip duration and demand**?
* 📆 Are there **peak travel patterns** during weekends or adverse weather conditions?

---

## 🧩📂 Data Sources

| Source                   | Format     | Description                               |
| ------------------------ | ---------- | ----------------------------------------- |
| 🚕 NYC Taxi Trip Records | Parquet    | High-volume taxi trip transaction data    |
| 🗺️ Taxi Zone Lookup     | CSV        | Borough and location mapping              |
| 🌦️ NYC Weather Data     | JSON (API) | Hourly temperature and weather conditions |

✔ **Requirement satisfied:** Includes at least one **Parquet-based** data source.

---

## 🏗️🧠 Architecture Overview

```
┌──────────────┐
│ Parquet Data │
└──────┬───────┘
       │
┌──────▼───────┐
│   CSV Data   │
└──────┬───────┘
       │
┌──────▼───────┐
│  JSON (API)  │
└──────┬───────┘
       │
┌──────▼────────────────────────┐
│ Apache PySpark (Transform)     │
│ - Cleaning                     │
│ - Enrichment                   │
│ - Feature Engineering          │
└──────┬────────────────────────┘
       │
┌──────▼─────────────┐
│ Parquet (Staging)  │
└──────┬─────────────┘
       │
┌──────▼─────────────┐
│ DuckDB (Analytics) │
└──────┬─────────────┘
       │
┌──────▼─────────────┐
│ BI Dashboard       │
│ (Power BI / etc.)  │
└────────────────────┘
```

---

## ⚙️🛠️ Technologies Used

| Category                 | Tool                        |
| ------------------------ | --------------------------- |
| ⚡ Distributed Processing | Apache PySpark              |
| 🧭 Orchestration         | Prefect                     |
| 💾 Storage               | Parquet                     |
| 🦆 Analytics Database    | DuckDB                      |
| 📊 BI Tool               | Power BI / Tableau / Looker |
| 🐍 Programming Language  | Python 3                    |
| 🪟 OS Support            | Windows-safe configuration  |

---

## 🔄🔧 ETL Pipeline Description

### 1️⃣ 📥 Extract

* Read **Parquet-based** NYC taxi trip records
* Load taxi zone lookup data from **CSV**
* Retrieve and parse weather data from a **JSON-based API**

---

### 2️⃣ 🔄 Transform (Apache PySpark)

* ❌ Filter invalid taxi trips (distance, fare, duration)
* 🧹 Handle missing and null values safely
* 🧠 Feature engineering:

  * ⏱️ Trip duration
  * 🕒 Pickup hour, weekday, weekend flag
  * 🌦️ Weather category
* 🔗 Enrich taxi trips with:

  * 🗺️ Borough information
  * ⏰ Hourly weather data
* 🛡️ Apply fallback logic for missing weather records

---

### 3️⃣ 📤 Load

* Write transformed datasets to **Parquet staging storage**
* Load Parquet files into **DuckDB** using `read_parquet()`
* Create **analytics-ready tables** for BI consumption

---

## ⏱️🧭 Pipeline Orchestration (Prefect)

The ETL pipeline is orchestrated using **Prefect**, providing:

* 🔁 Task-level fault tolerance
* ♻️ Automatic retries with delay
* 🧠 Daily caching to avoid redundant computation
* 🔗 Explicit task dependency management

The pipeline is structured as a **Prefect Flow**, with independent tasks for:

* PySpark transformation and enrichment
* DuckDB loading and validation

---

## ⏱️🏠 Local Prefect Setup (Development & Academic Use)

### 1️⃣ ▶️ Start Prefect Server

```powershell
prefect server start
```

UI available at:

```
http://127.0.0.1:4200
```

---

### 2️⃣ 📂 Navigate to Project Directory

```powershell
cd C:\Users\HP\Documents\etl-project
```

---

### 3️⃣ 🏗️ Create Work Pool (One-Time Setup)

```powershell
prefect work-pool create local-pool --type process
```

---

### 4️⃣ 🗓️ Deploy the Flow with Schedule

```powershell
prefect deploy prefect_flow.py:main_flow --name daily_etl --pool local-pool --cron "0 9 * * *"
```

✔ Scheduled to run **daily at 09:00**

---

### 5️⃣ 🧑‍💻 Start Prefect Worker

```powershell
prefect worker start --pool local-pool
```

---

testing screenshots of the terminal
<img width="1460" height="427" alt="Screenshot 2025-12-15 045758" src="https://github.com/user-attachments/assets/c27108ff-9365-4f08-82f5-085cf9315b64" />

<img width="1316" height="403" alt="Screenshot 2025-12-15 044211" src="https://github.com/user-attachments/assets/5e9485fd-fa0f-4689-ad56-fcb91c1e0781" />


### 6️⃣ 🔍 Monitoring

Using the Prefect UI, you can:

* 👀 Monitor flow execution
* 📜 Inspect logs
* 🔄 Track retries and failures

---

## 📊🖼️ BI Dashboard (With Images)

The DuckDB database is connected to a BI tool to visualize:

* ⏰ Taxi trips by hour and weekday
* 🌦️ Average trip duration by weather condition
* 📆 Demand comparison: weekday vs weekend
* 🌡️ Temperature impact on taxi usage

### 📸 Dashboard Screenshots

> Place your images inside `bi/dashboard_screenshots/`

```md
![Taxi Trips by Hour](bi/dashboard_screenshots/trips_by_hour.png)
![Average Trip Duration by Weather](bi/dashboard_screenshots/trip_duration_weather.png)
![Weekday vs Weekend Demand](bi/dashboard_screenshots/weekday_weekend.png)
![Temperature Impact on Demand](bi/dashboard_screenshots/temperature_impact.png)
```

✅ GitHub will render these images automatically once the files exist.

---
Perfect, Yonas 👍
You **do NOT need to remove or reduce anything** from your README.
Below is a **clean, polished, copy-paste ready Markdown section** that fits **exactly before** the **📁🗂️ Repository Structure** section.

I only **refined wording, headings, and formatting** — the meaning and content remain **100% yours**.

---

## 🔧📐 dbt (Data Build Tool) Integration

### Overview

In addition to the core ETL pipeline, this project integrates **dbt (Data Build Tool)** to perform **advanced transformation modeling** directly within **DuckDB**.

dbt is used to organize SQL-based transformations into **structured, testable, and reusable data models**, following modern **analytics engineering best practices**.
This integration enhances **data quality**, **maintainability**, and **analytical clarity**, and fulfills the **bonus requirement** for advanced transformation modeling.

---

### Why dbt Was Used

While **Apache PySpark** handles large-scale data extraction, cleaning, and enrichment, **dbt is applied after data is loaded into DuckDB** to:

* Structure transformations into **staging and mart layers**
* Define **clear model dependencies**
* Enable **SQL-based analytical modeling**
* Improve **reproducibility and documentation**
* Separate **ETL processing** from **analytics logic**

This approach reflects **real-world data warehouse design patterns** used in industry.

---

### dbt Position in the Pipeline

```
Raw Data Sources (Parquet, CSV, JSON)
        ↓
PySpark Transformations (ETL)
        ↓
DuckDB (Processed Analytical Store)
        ↓
dbt Models (Staging → Mart)
        ↓
BI Dashboard (Power BI)
```

✔ dbt operates **on top of DuckDB** and **does not replace** the ETL pipeline.

---

### dbt Models Implemented

#### 1️⃣ Staging Model

A staging model was created to standardize and expose cleaned data from DuckDB.

* **Model name:** `stg_taxi_weather`
* **Purpose:** Provide a clean, structured analytical view
* **Source:** DuckDB tables generated by the ETL pipeline

Key fields include:

* Trip distance
* Fare amount
* Trip duration
* Weather category
* Pickup date and time

---

#### 2️⃣ Mart (Analytics) Model

An analytical mart model was created to summarize taxi activity.

* **Model name:** `mart_trips_summary`
* **Purpose:** Aggregate taxi trips by pickup location

**Metrics produced:**

* Total number of trips
* Average trip distance
* Average fare amount
* Average trip duration

This model demonstrates **advanced transformation modeling** and serves as a **direct input for BI dashboards**.

---

### Data Quality & Testing

Basic data quality validation was implemented using **dbt tests**, including:

* `not_null` checks on critical fields such as **`PULocationID`**

These tests ensure **data reliability** before analytics and reporting.

---

### Impact on Existing Dashboard

The dbt integration was designed to be **non-disruptive**:

* Existing **Power BI dashboards continue to work without modification**
* dbt-generated models can optionally be used as **optimized analytical sources**
* No changes were required to existing **ETL logic or DuckDB schemas**

---

### Benefits of dbt Integration

* Clear separation of **data engineering (ETL)** and **analytics engineering (modeling)**
* Improved readability and maintainability of SQL transformations
* Industry-aligned modeling practices
* Scalable and reusable analytics layer
* Satisfies the **bonus requirement** for advanced transformation modeling

---

### Summary

By integrating **dbt with DuckDB**, this project demonstrates a **complete modern data pipeline** that combines distributed processing, analytical storage, orchestration, and structured analytics modeling.
This design closely mirrors **real-world production data engineering workflows** and strengthens both **technical robustness** and **analytical usability**.

---



## 📁🗂️ Repository Structure

```
├── data/
│   ├── raw/
│   │   ├── taxi_parquet/
│   │   ├── taxi_zone_lookup.csv
│   │   └── weather/
│   ├── staging_parquet/
│   └── processed/
│       └── taxi_weather.duckdb
│
├── prefect_flow.py
├── etl_pipeline.ipynb
├── README.md
│
└── bi/
    └── dashboard_screenshots/
```

---

## 🧪✅ Data Quality Checks

* 🔍 Null value validation
* ❌ Invalid trip filtering
* 🌦️ Weather fallback logic
* 🧾 Schema inspection
* 📊 Row and column verification in DuckDB

---

## 👥🤝 Team Members & Roles

| Name              | Role / Contribution                                            |
| ----------------- | -------------------------------------------------------------- |
| Mikiyas Tolko     | Project Lead; ETL architecture; PySpark; Prefect orchestration |
| Demirew Manidefro | DuckDB integration; schema validation                          |
| Lamrot Solomon    | Weather API integration; JSON parsing                          |
| Nahom Teshome     | Data cleaning; null handling; duration calculations            |
| Yonas Habtamu     | BI dashboard design; taxi zone mapping                         |
| Abaynewu Aberu    | Documentation; README preparation; coordination                |
| Yonas Abebe       | Testing; validation; data quality checks                       |

---

## 🚀▶️ How to Run the Project

### 1️⃣ 📦 Install Dependencies

```bash
pip install pyspark duckdb prefect findspark requests
```

### 2️⃣ 🔐 Set Environment Variable

```bash
export WEATHER_API_KEY="your_api_key"
```

### 3️⃣ ▶️ Run ETL Pipeline

```bash
python prefect_flow.py
```

---
