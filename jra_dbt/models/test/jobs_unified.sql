{{ 
    config(
        materialized='incremental',
        file_format='delta',
        location_root='s3://jla-data-silver/',
        unique_key='job_posting_id',
        incremental_strategy='merge'
    ) 
}}


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
    FROM {{ source('silver_layer', 'linkedin_cleaned') }}

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
    FROM {{ source('silver_layer', 'indeed_cleaned') }}

)


SELECT * FROM source_unioned

-- 第 3 步：添加增量加载的过滤条件
{% if is_incremental() %}

  -- 这个 WHERE 条件只会在增量运行时被应用
  -- 它会从合并后的数据中，只选择 ingest_dts 大于目标表中已存在的最大 ingest_dts 的记录
  WHERE ingest_dts > (SELECT max(ingest_dts) FROM {{ this }})

{% endif %}