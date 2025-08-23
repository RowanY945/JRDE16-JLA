-- back compat for old kwarg name
  
  
  
  
  
  
      
          
          
      
  

    merge
    into
        `demo`.`demo_schema`.`stg_jobs_description_new` as DBT_INTERNAL_DEST
    using
        `stg_jobs_description_new__dbt_tmp` as DBT_INTERNAL_SOURCE
    on
        
              DBT_INTERNAL_SOURCE.job_posting_id <=> DBT_INTERNAL_DEST.job_posting_id
          
    when matched
        then update set
            *
    when not matched
        then insert
            *

