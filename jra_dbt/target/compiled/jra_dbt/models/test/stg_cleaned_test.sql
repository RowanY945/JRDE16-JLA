

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
    base_salary.min_amount AS min_salary, 
    base_salary.max_amount AS max_salary,
    date_format(to_date(job_posted_date, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\''), 'yyyy-MM-dd') AS job_posted_date, 
    timestamp AS scraped_dts, 
    ingest_dts,
    _rescued_data,
    0 AS is_enriched, 
    1 AS job_source, 
    1 AS is_active,
    -- Partition columns based on scraped_time
    year(current_date()) as year,
    month(current_date()) as month,
    day(current_date()) as day

FROM `demo`.`demo_schema`.`stg_test2`


    WHERE timestamp > (
        SELECT COALESCE(MAX(ingest_dts), '1900-01-01'::timestamp)
        FROM `demo`.`demo_schema`.`stg_cleaned_test`
    )
