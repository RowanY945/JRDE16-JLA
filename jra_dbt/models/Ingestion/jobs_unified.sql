{{ 
    config(
        materialized='table',
        file_format='parquet',
        location_root='s3://jla-data-silver'
    ) 
}}
SELECT DISTINCT url, job_posting_id, job_title, company_name, job_location,
job_seniority_level, job_function, job_employment_type, job_industries,
job_posted_date,
base_salary,scraped_time, _rescued_data,is_enriched,job_source,is_active,source_file,ingest_dts
FROM {{ source('linkedin_ingestion', 'jobs_linkedin_cleaned') }} AS sr