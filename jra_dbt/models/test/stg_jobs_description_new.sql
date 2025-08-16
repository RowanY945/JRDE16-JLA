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
    
    job_posting_id, job_summary,scraped_dts
    
FROM {{ source('silver_layer', 'stg_cleaned_test') }}

{% if is_incremental() %}
    WHERE scraped_dts > (
        SELECT COALESCE(MAX(scraped_dts), '1900-01-01'::timestamp)
        FROM {{ this }}
    )
{% endif %}