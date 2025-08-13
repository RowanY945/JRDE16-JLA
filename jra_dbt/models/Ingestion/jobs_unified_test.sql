{{ 
    config(
        materialized='incremental',
        file_format='delta',
        location_root='s3://jla-data-silver/',
        unique_key='job_posting_id',
        incremental_strategy='merge'
    ) 
}}

SELECT 
    url, 
    job_posting_id, 
    job_title, 
    company_name, 
    job_location,
    job_seniority_level, 
    job_function, 
    job_employment_type, 
    job_industries,
    min_salary,
    max_salary,
    job_posted_date,
    scraped_dts, 
    ingest_dts,
    _rescued_data,
    is_enriched,
    job_source,
    is_active
FROM {{ source('linkedin_ingestion', 'jobs_linkedin_cleaned') }} AS sr

{% if is_incremental() %}
    WHERE ingest_dts > (
        SELECT COALESCE(MAX(ingest_dts), '1900-01-01'::timestamp)
        FROM {{ this }}
    )
{% endif %}
