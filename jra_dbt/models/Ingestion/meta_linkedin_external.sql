{{ config(materialized='table',catalog='demo') }}

SELECT DISTINCT url, job_posting_id, job_title, company_name, job_location, job_summary,
job_seniority_level, job_function, job_employment_type, job_industries,
date_format(to_date(job_posted_date, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\''), 'yyyy-MM-dd') AS job_posted_date,
base_salary, timestamp AS scraped_time, _rescued_data,0 AS is_enriched, 1 AS job_source, 1 AS is_active,source_file,ingest_dts
FROM {{ source('linkedin_ingestion', 'stg_linkedin_external') }} AS sr