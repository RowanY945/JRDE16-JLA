
    
  create or replace table `demo`.`demo_schema`.`stg_unified_test`
  
  (
    
      url string,
    
      job_posting_id string,
    
      job_title string,
    
      company_name string,
    
      job_location string,
    
      job_seniority_level string,
    
      job_function string,
    
      job_employment_type string,
    
      job_industries string,
    
      min_salary double,
    
      max_salary double,
    
      job_posted_date string,
    
      scraped_dts timestamp,
    
      ingest_dts timestamp,
    
      _rescued_data string,
    
      is_enriched int,
    
      job_source int,
    
      is_active int
    
    
  )

  
  using delta
  
  
  
  
  location 's3://jla-data-silver/stg_unified_test'
  
  

  