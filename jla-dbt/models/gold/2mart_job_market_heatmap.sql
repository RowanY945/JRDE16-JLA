
{{ 
    config(
        materialized='incremental',
        file_format='delta',
        location_root='s3://jla-data-gold/',
        unique_key='job_posting_id',
        incremental_strategy='merge'
    ) 
}}


with source_data as (
    select * from {{ source('gold_layer', 'stg_job_postings') }}
),

-- Add standardization step for job_seniority_level
standardized_data as (
select *,
    case 
        when upper(job_seniority_level) = 'MID_SENIOR_LEVEL' 
            or upper(job_seniority_level) = 'MID-SENIOR LEVEL' then 'Mid-Senior level'
        when upper(job_seniority_level) = 'ENTRY_LEVEL' 
            or upper(job_seniority_level) = 'ENTRY LEVEL' then 'Entry level'
        when upper(job_seniority_level) = 'SENIOR_LEVEL' 
            or upper(job_seniority_level) = 'SENIOR LEVEL' then 'Senior level'
        when upper(job_seniority_level) = 'ASSOCIATE' then 'Associate'
        when upper(job_seniority_level) = 'EXECUTIVE' then 'Executive'
        when upper(job_seniority_level) = 'DIRECTOR' then 'Director'
        when upper(job_seniority_level) = 'INTERNSHIP' then 'Internship'
        else initcap(replace(job_seniority_level, '_', ' '))  -- fallback for any other variants
    end as standardized_job_seniority_level
from {{ ref('stg_job_postings') }}
),
job_density as (
select
 job_city,
 job_state,
 job_industries,
 standardized_job_seniority_level as job_seniority_level,  -- Use standardized version
 job_employment_type,
count(*) as job_count,
count(distinct company_name) as unique_companies,
avg(case when min_salary is not null then min_salary end) as avg_min_salary,
avg(case when max_salary is not null then max_salary end) as avg_max_salary,
count(case when job_posted_date >= current_date - interval 7 days then 1 end) as jobs_last_7_days,
count(case when job_posted_date >= current_date - interval 30 days then 1 end) as jobs_last_30_days
from standardized_data
where job_city is not null
group by 1,2,3,4,5
),
market_temperature as (
select *,
-- Market heat indicators
case
when jobs_last_7_days >= 5 then 'HOT'
when jobs_last_7_days >= 2 then 'WARM'
when jobs_last_30_days >= 5 then 'MODERATE'
else 'COOL'
end as market_temperature,
-- Competitiveness score (0-100)
least(100, round((job_count * 10.0 / nullif(unique_companies, 0)) +
 (jobs_last_7_days * 5), 0)) as competitiveness_score,
-- Opportunity index
round(ln(job_count + 1) * ln(unique_companies + 1) *
case when avg_max_salary > 0 then ln(avg_max_salary/50000 + 1) else 1 end, 2) as opportunity_index
from job_density
)
select * from market_temperature