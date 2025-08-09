{{ 
    config(
        materialized='table',
        file_format='parquet',
        location_root='s3://jla-data-silver/'
    ) 
}}



SELECT DISTINCT  job_posting_id, job_summary
FROM {{ source('linkedin_ingestion', 'jobs_linkedin_cleaned') }} AS sr