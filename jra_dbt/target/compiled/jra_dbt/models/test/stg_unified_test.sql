

-- 第 1 步：将 UNION ALL 的逻辑放到一个 CTE (Common Table Expression) 中
with source_unioned as (

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
    FROM `demo`.`demo_schema`.`stg_cleaned_test`

    UNION ALL

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
    FROM `demo`.`demo_schema`.`indeed_cleaned`

)


SELECT * FROM source_unioned

-- 第 3 步：添加增量加载的过滤条件
