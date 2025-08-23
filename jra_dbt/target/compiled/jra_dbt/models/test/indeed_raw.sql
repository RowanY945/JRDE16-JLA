

SELECT 
  *,
  current_timestamp() AS ingest_dts,
  _metadata.file_path AS source_file,
  -- Add partition columns
  year(current_date()) as year,
  month(current_date()) as month,
  day(current_date()) as day
FROM read_files(
  's3://jla-raw-datalake/raw/Indeed/',
  format => 'parquet'
)


    -- Only read files modified since last run
    WHERE timestamp > (
        SELECT COALESCE(MAX(timestamp), '1900-01-01'::timestamp) 
        FROM `demo`.`demo_schema`.`indeed_raw`
    )
