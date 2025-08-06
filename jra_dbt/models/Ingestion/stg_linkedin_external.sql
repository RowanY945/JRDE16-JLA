SELECT *,current_date() AS ingest_dts,_metadata.file_path AS source_file
FROM read_files(
  's3://jla-raw-datalake/raw/linkedin/',
  format => 'parquet'
)