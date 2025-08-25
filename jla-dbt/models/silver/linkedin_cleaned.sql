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


WITH parsed_data AS (
    SELECT 
        url, 
        job_posting_id, 
        job_title, 
        company_name, 
        job_location,
        -- Parse city/suburb from job_location
        CASE 
            -- Handle "Greater/Area" locations
            WHEN job_location LIKE '%Greater % Area%' 
                THEN REGEXP_EXTRACT(job_location, 'Greater (.*) Area', 1)
            WHEN job_location LIKE '% Area'
                THEN TRIM(REPLACE(job_location, 'Area', ''))
            
            -- Handle abbreviated state formats
            WHEN job_location REGEXP ', (VIC|NSW|QLD|SA|WA|TAS|NT|ACT), AU' 
                THEN SPLIT(job_location, ',')[0]
            
            -- Handle state-only or country-only entries
            WHEN job_location IN ('Queensland, Australia', 'Victoria, Australia', 
                                 'New South Wales, Australia', 'Tasmania, Australia',
                                 'Northern Territory, Australia', 'Australian Capital Territory, Australia',
                                 'Western Australia, Australia', 'South Australia, Australia')
                THEN NULL
            WHEN job_location = 'Australia' 
                THEN NULL
                
            -- Standard city/suburb extraction
            WHEN job_location LIKE '%,%,%'
                THEN TRIM(SPLIT(job_location, ',')[0])
            WHEN job_location LIKE '%,%'
                AND job_location NOT IN ('Queensland, Australia', 'Victoria, Australia', 
                                        'New South Wales, Australia', 'Tasmania, Australia',
                                        'Northern Territory, Australia', 'Australian Capital Territory, Australia',
                                        'Western Australia, Australia', 'South Australia, Australia')
                THEN TRIM(SPLIT(job_location, ',')[0])
            ELSE NULL
        END AS job_city,
        
        -- Parse state and convert to abbreviation
        CASE
            -- Already abbreviated states
            WHEN job_location REGEXP ', VIC, ' THEN 'VIC'
            WHEN job_location REGEXP ', NSW, ' THEN 'NSW'
            WHEN job_location REGEXP ', QLD, ' THEN 'QLD'
            WHEN job_location REGEXP ', SA, ' THEN 'SA'
            WHEN job_location REGEXP ', WA, ' THEN 'WA'
            WHEN job_location REGEXP ', TAS, ' THEN 'TAS'
            WHEN job_location REGEXP ', NT, ' THEN 'NT'
            WHEN job_location REGEXP ', ACT, ' THEN 'ACT'
            
            -- Full state names to abbreviations
            WHEN job_location LIKE '%Victoria%' THEN 'VIC'
            WHEN job_location LIKE '%New South Wales%' THEN 'NSW'
            WHEN job_location LIKE '%Queensland%' THEN 'QLD'
            WHEN job_location LIKE '%Western Australia%' THEN 'WA'
            WHEN job_location LIKE '%South Australia%' THEN 'SA'
            WHEN job_location LIKE '%Tasmania%' THEN 'TAS'
            WHEN job_location LIKE '%Northern Territory%' THEN 'NT'
            WHEN job_location LIKE '%Australian Capital Territory%' THEN 'ACT'
            
            -- Australia only case
            WHEN job_location = 'Australia' THEN NULL
            
            ELSE NULL
        END AS job_state,
        
        -- Set country (assuming all Australian data)
        CASE 
            WHEN job_location IS NOT NULL AND job_location != '' THEN 'AU'
            ELSE NULL
        END AS job_country,
        job_summary,
        job_seniority_level, 
        job_function, 
        job_employment_type, 
        job_industries,
        base_salary.min_amount AS min_salary, 
        base_salary.max_amount AS max_salary,
        date_format(to_date(job_posted_date, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\''), 'yyyy-MM-dd') AS job_posted_date, 
        timestamp AS scraped_dts, 
        ingest_dts,
        _rescued_data,
        CAST(0 AS BOOLEAN) AS is_enriched, 
        1 AS job_source, 
        CAST(1 AS BOOLEAN) AS is_active,
        -- Partition columns based on scraped_time
        year(current_date()) as year,
        month(current_date()) as month,
        day(current_date()) as day        
    FROM {{ source('bronze_layer', 'linkedin_jobs') }}

    {% if is_incremental() %}
        WHERE ingest_dts > (
            SELECT COALESCE(MAX(ingest_dts), '1900-01-01'::timestamp)
            FROM {{ this }}
        )
    {% endif %}
)

SELECT 
    url, 
    job_posting_id, 
    job_title, 
    company_name, 
    job_location,  -- Keep original for reference/debugging if needed
    job_city,      -- New parsed field
    job_state,     -- New parsed field  
    job_country,   -- New parsed field
    job_summary,
    job_seniority_level, 
    job_function, 
    job_employment_type, 
    job_industries,
    min_salary, 
    max_salary, 
    job_posted_date, 
    scraped_dts, 
    ingest_dts,
    _rescued_data,
    is_enriched, 
    job_source, 
    is_active,
    year,
    month,
    day
FROM parsed_data