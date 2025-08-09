{{ 
    config(
        materialized='table',
        file_format='parquet',
        location_root='s3://jla-data-bronze/',
        partition_by=['year', 'month', 'day']
    ) 
}}



SELECT DISTINCT url, job_posting_id, job_title, company_name, job_location, job_summary,
job_seniority_level, job_function, job_employment_type, job_industries,
date_format(to_date(job_posted_date, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\''), 'yyyy-MM-dd') AS job_posted_date,
base_salary, timestamp AS scraped_time, _rescued_data,0 AS is_enriched, 1 AS job_source, 1 AS is_active,source_file,ingest_dts,year(current_date()) as year,
  month(current_date()) as month,
  day(current_date()) as day
FROM {{ source('linkedin_ingestion', 'linkedin_jobs') }} AS sr