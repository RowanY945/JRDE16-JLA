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
    job_posting_id, 
    job_seniority_level,
    workplace_type,
    min_salary,
    max_salary,
    it_domains,
    ai_summaries,
    summarized_dts

FROM read_files(
  's3://jla-ai-outputs/summaries/',
  format => 'parquet'
)

{% if is_incremental() %}
    -- Only read files modified since last run
    WHERE summarized_dts > (
        SELECT COALESCE(MAX(summarized_dts), '1900-01-01'::timestamp) 
        FROM {{ this }}
    )
{% endif %}