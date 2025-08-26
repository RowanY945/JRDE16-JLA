
-- models/gold/mart_company_hiring_intelligence.sql
{{ 
    config(
        materialized='incremental',
        file_format='delta',
        location_root='s3://jla-data-gold/',
        unique_key='company_name',
        incremental_strategy='merge',
        tags=['analytics', 'company_intelligence']
    ) 
}}


with source_data as (
    select * from {{ source('gold_layer', 'stg_job_postings') }}
),

with company_metrics as (
    select
        company_name,
        job_industries,
        
        -- Hiring volume metrics
        count(*) as total_job_postings,
        count(distinct job_title) as unique_roles,
        count(distinct job_function) as function_diversity,
        count(distinct job_city) as geographic_spread,
        
        -- Temporal patterns
        count(case when job_posted_date >= current_date - interval 7 days then 1 end) as jobs_last_7d,
        count(case when job_posted_date >= current_date - interval 30 days then 1 end) as jobs_last_30d,
        count(case when job_posted_date >= current_date - interval 90 days then 1 end) as jobs_last_90d,
        
        -- Employment type strategy
        sum(case when job_employment_type = 'Full-time' then 1 else 0 end) as fulltime_roles,
        sum(case when job_employment_type = 'Contract' then 1 else 0 end) as contract_roles,
        sum(case when job_employment_type = 'Part-time' then 1 else 0 end) as parttime_roles,
        
        -- Seniority distribution
        sum(case when job_seniority_level in ('Entry level', 'Associate') then 1 else 0 end) as junior_roles,
        sum(case when job_seniority_level = 'Mid-Senior level' then 1 else 0 end) as midsenior_roles,
        sum(case when job_seniority_level in ('Director', 'Executive') then 1 else 0 end) as leadership_roles,
        
        -- Salary investment (where available)
        avg(case when min_salary > 0 then min_salary end) as avg_min_salary,
        avg(case when max_salary > 0 then max_salary end) as avg_max_salary,
        
        min(job_posted_date) as first_posting_date,
        max(job_posted_date) as latest_posting_date
        
    from {{ ref('stg_job_postings') }}
    {% if is_incremental() %}
    where dbt_updated_at > (select max(dbt_updated_at) from {{ this }})
    {% endif %}
    group by 1,2
),

company_intelligence as (
    select *,
        -- Hiring velocity indicators
        round(jobs_last_30d * 12.0 / greatest(jobs_last_90d, 1), 1) as hiring_velocity_score,
        
        -- Growth stage indicators  
        case
            when jobs_last_7d >= 10 then 'RAPID_EXPANSION'
            when jobs_last_30d >= 15 then 'HIGH_GROWTH'
            when jobs_last_30d >= 5 then 'STEADY_GROWTH'
            when jobs_last_90d >= 5 then 'MODERATE_HIRING'
            else 'LIMITED_HIRING'
        end as hiring_stage,
        
        -- Hiring strategy classification
        case
            when contract_roles > fulltime_roles then 'CONTRACT_FOCUSED'
            when leadership_roles > (junior_roles + midsenior_roles) then 'LEADERSHIP_HEAVY'
            when junior_roles > (midsenior_roles + leadership_roles) then 'TALENT_DEVELOPMENT'
            else 'BALANCED_HIRING'
        end as hiring_strategy,
        
        -- Organizational complexity score
        round(sqrt(unique_roles) * sqrt(geographic_spread) * log(function_diversity + 1), 1) as complexity_score,
        
        -- Market presence indicator
        case
            when total_job_postings >= 20 then 'MAJOR_PLAYER'
            when total_job_postings >= 10 then 'ACTIVE_HIRER'
            when total_job_postings >= 5 then 'REGULAR_HIRER'
            else 'SELECTIVE_HIRER'
        end as market_presence
        
    from company_metrics
)

select * from company_intelligence