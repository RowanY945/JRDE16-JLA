

SELECT 
    
    job_posting_id, job_summary,scraped_dts
    
FROM `demo`.`demo_schema`.`stg_cleaned_test`

UNION ALL 

SELECT 
    
    job_posting_id, job_summary,scraped_dts
    
FROM `demo`.`demo_schema`.`indeed_cleaned`


    WHERE scraped_dts > (
        SELECT COALESCE(MAX(scraped_dts), '1900-01-01'::timestamp)
        FROM `demo`.`demo_schema`.`stg_jobs_description_new`
    )
