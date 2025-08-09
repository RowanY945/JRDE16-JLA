{{ 
    config(
        materialized='table',
        file_format='parquet',
        location_root='s3://jla-data-bronze/',
        partition_by=['year', 'month', 'day']
    ) 
}}

SELECT *,current_date() AS ingest_dts,_metadata.file_path AS source_file,year(current_date()) as year,
  month(current_date()) as month,
  day(current_date()) as day
FROM read_files(
  's3://jla-raw-datalake/raw/linkedin/',
  format => 'parquet'
)