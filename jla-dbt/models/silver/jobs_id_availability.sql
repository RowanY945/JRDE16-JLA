{{ 
    config(
        materialized='incremental',
        file_format='delta',
        location_root='s3://jla-data-silver/',
        unique_key='job_posting_id',
        incremental_strategy='merge',
        partition_by=['year', 'month', 'day']
    ) 
}}

SELECT 
  CAST(job_posting_id AS STRING) AS job_posting_id,
  CAST(is_active AS BOOLEAN) AS is_active,
  validated_dts,
  -- Add partition columns
  year(current_date()) as year,
  month(current_date()) as month,
  day(current_date()) as day
FROM read_files(
  's3://jla-jobids-pool',
  format => 'csv'
)

{% if is_incremental() %}
    -- Only read files modified since last run
    WHERE validated_dts > (
        SELECT COALESCE(MAX(validated_dts), '1900-01-01'::timestamp) 
        FROM {{ this }}
    )
{% endif %}