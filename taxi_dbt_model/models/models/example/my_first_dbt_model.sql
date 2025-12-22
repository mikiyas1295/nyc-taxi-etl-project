{{ config(materialized='table') }}

with source_data as (
    select 1 as id  -- only non-null rows
    -- you can add more rows here, just make sure id is never null
)

select *
from source_data