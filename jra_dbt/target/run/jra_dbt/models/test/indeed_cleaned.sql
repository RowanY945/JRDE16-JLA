
    
  create or replace table `demo`.`silver`.`indeed_cleaned`
  
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
    
      min_salary double,
    
      max_salary double,
    
      job_posted_date string,
    
      scraped_dts timestamp_ntz,
    
      ingest_dts timestamp,
    
      _rescued_data string,
    
      is_enriched int,
    
      job_source int,
    
      is_active int,
    
      year int,
    
      month int,
    
      day int
    
    
  )

  
  using delta
  
  partitioned by (year,month,day)
  
  
  location 's3://jla-data-silver/indeed_cleaned'
  
  

  