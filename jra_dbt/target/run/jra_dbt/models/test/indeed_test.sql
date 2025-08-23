
    
  create or replace table `demo`.`demo_schema`.`indeed_test`
  
  (
    
      url string,
    
      job_posting_id string,
    
      job_title string,
    
      company_name string,
    
      job_location string,
    
      job_summary string,
    
      job_seniority_level string,
    
      job_function string,
    
      job_employment_type string,
    
      job_industries string,
    
      job_posted_date string,
    
      MIN_AMOUNT double,
    
      MAX_AMOUNT double,
    
      timestamp timestamp_ntz,
    
      _rescued_data string,
    
      ingest_dts timestamp,
    
      source_file string,
    
      year int,
    
      month int,
    
      day int
    
    
  )

  
  using delta
  
  partitioned by (year,month,day)
  
  
  location 's3://jla-data-bronze/indeed_test'
  
  

  