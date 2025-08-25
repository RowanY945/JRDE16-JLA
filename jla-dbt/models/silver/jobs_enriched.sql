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
    -- All columns from jobs_unified
    ju.url,
    ju.job_posting_id,
    ju.job_title,
    ju.company_name,
    ju.job_city,
    ju.job_state,
    ju.job_country,
    
    -- job_seniority_level with conditional logic
    CASE 
        -- When job_source = 0, use value from jobs_ai_summaries
        WHEN ju.job_source = 0 THEN COALESCE(jas.job_seniority_level, ju.job_seniority_level)
        -- When NULL or 'Not Applicable' in jobs_unified, use value from jobs_ai_summaries
        WHEN ju.job_seniority_level IS NULL OR ju.job_seniority_level = 'Not Applicable' 
            THEN COALESCE(jas.job_seniority_level, ju.job_seniority_level)
        ELSE ju.job_seniority_level
    END AS job_seniority_level,
    
    ju.job_function,
    ju.job_employment_type,
    ju.job_industries,
    
    -- min_salary with replacement logic
    CASE 
        WHEN ju.min_salary IS NULL OR ju.min_salary = 'Not Applicable' 
            THEN COALESCE(jas.min_salary, ju.min_salary)
        ELSE ju.min_salary
    END AS min_salary,
    
    -- max_salary with replacement logic
    CASE 
        WHEN ju.max_salary IS NULL OR ju.max_salary = 'Not Applicable' 
            THEN COALESCE(jas.max_salary, ju.max_salary)
        ELSE ju.max_salary
    END AS max_salary,
    
    ju.job_posted_date,
    ju.scraped_dts,
    ju.ingest_dts,
    ju._rescued_data,
    
    -- Set is_enriched to true (1) as specified
    true AS is_enriched,
    
    -- Add enriched_dts timestamp for tracking when enrichment happened
    current_timestamp() AS enriched_dts,
    
    ju.job_source,
    
    -- is_active from jobs_id_availability (prioritized over jobs_unified)
    COALESCE(jia.is_active, ju.is_active) AS is_active,
    
    -- New columns from jobs_ai_summaries
    jas.workplace_type,  -- This column only exists in jobs_ai_summaries
    jas.it_domains,
    jas.ai_summaries

FROM {{ ref('jobs_unified') }} ju

-- Left join with jobs_id_availability for is_active status
LEFT JOIN {{ ref('jobs_id_availability') }} jia 
    ON ju.job_posting_id = jia.job_posting_id

-- Left join with jobs_ai_summaries for enrichment data
LEFT JOIN {{ ref('jobs_ai_summaries') }} jas 
    ON ju.job_posting_id = jas.job_posting_id

-- Filter to only process jobs that haven't been enriched
WHERE ju.is_enriched = false

-- Incremental filter based on enriched_dts
{% if is_incremental() %}
    AND ju.ingest_dts > (SELECT max(enriched_dts) FROM {{ this }})
{% endif %}