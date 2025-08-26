{{ 
    config(
        materialized='incremental',
        file_format='delta',
        location_root='s3://jla-data-gold/',
        unique_key='job_posting_id',
        incremental_strategy='merge'
    ) 
}}

with source_data as (
    select * from {{ source('silver_layer', 'jobs_enriched') }}
),

cleaned_data as (
    select
        -- Primary identifiers
        job_posting_id,
        url as job_url,
        
        -- Job details
        job_title,
        company_name,
        job_city,
        job_state,
        job_country,
        job_seniority_level,
        job_function,
        job_posted_date,
        job_employment_type,
        job_industries,
        
        -- Date transformations
        
        scraped_dts,
        ingest_dts,
        _rescued_data,


        -- Salary parsing (basic extraction)
        min_salary,
        max_salary,
        

        -- Status flags
        case when is_enriched = 1 then true else false end as is_enriched,
        case when is_active = 1 then true else false end as is_active,
        job_source,
        enriched_dts,


        
        -- Audit fields
        current_timestamp() as dbt_created_at,
        current_timestamp() as dbt_updated_at
        
    from source_data
    where is_active = 1  -- Only include active job postings
)

select * from cleaned_data





{% if is_incremental() %}
    -- Only read files modified since last run
    WHERE enriched_dts > (
        SELECT COALESCE(MAX(enriched_dts), '1900-01-01'::timestamp) 
        FROM {{ this }}
    )
{% endif %}