{{ 
    config(
        materialized='incremental',
        file_format='delta',
        location_root='s3://jla-data-silver/',
        unique_key='job_posting_id',
        incremental_strategy='merge'
    ) 
}}


SELECT job_posting_id, job_summary
FROM {{ source('linkedin_ingestion', 'jobs_linkedin_cleaned') }} AS sr

{% if is_incremental() %}
    WHERE ingest_dts > (
        SELECT COALESCE(MAX(ingest_dts), '1900-01-01'::timestamp)
        FROM {{ this }}
    )
{% endif %}