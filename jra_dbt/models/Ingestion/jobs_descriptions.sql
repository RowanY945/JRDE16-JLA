{{ 
    config(
        materialized='view',
        file_format='delta',
        location_root='s3://jla-data-silver/'
    ) 
}}



SELECT job_posting_id, job_summary
FROM {{ source('linkedin_ingestion', 'jobs_linkedin_cleaned') }} AS sr