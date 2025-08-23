{{ 
    config(
        materialized='incremental',
        file_format='delta',
        location_root='s3://jla-data-silver/',
        unique_key='job_posting_id',
        incremental_strategy='merge',
        partition_by=['year', 'month', 'day']
    ) 
}}

SELECT 
    url, 
    job_posting_id, 
    job_title, 
    company_name, 
    job_location, 
    job_summary,
    job_seniority_level, 
    job_function, 
    job_employment_type, 

    job_industries,
    
    MIN_AMOUNT as min_salary, 
    MAX_AMOUNT as max_salary, 
    date_format(to_date(cast(job_posted_date As timestamp), 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\''), 'yyyy-MM-dd') AS job_posted_date, 
    timestamp AS scraped_dts, 
    ingest_dts,
    _rescued_data,
    0 AS is_enriched, 
    0 AS job_source, 
    1 AS is_active,
    -- Partition columns based on scraped_time
    year(current_date()) as year,
    month(current_date()) as month,
    day(current_date()) as day

FROM {{ source('bronze_layer', 'indeed_test') }}

{% if is_incremental() %}
    WHERE timestamp > (
        SELECT COALESCE(MAX(ingest_dts), '1900-01-01'::timestamp)
        FROM {{ this }}
    )
{% endif %}