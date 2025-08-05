{{ 
  config(
    materialized='external',
    location='s3://jla-raw-datalake/raw/linkedin/',
    file_format='parquet'
  ) 
}}

select * from {{ source('linkedin_raw', 'linkedin_external') }}