
-- models/gold/mart_tech_skills_demand.sql
{{ 
    config(
        materialized='table',
        file_format='delta',
        location_root='s3://jla-data-gold/',
        tags=['analytics', 'skills', 'predictive']
    ) 
}}

with source_data as (
    select * from {{ source('gold_layer', 'stg_job_postings') }}
),

tech_role_classification as (
    select *,
        -- Classify tech roles and extract implied skills
        case 
            when lower(job_title) like '%data engineer%' then 'Data Engineering'
            when lower(job_title) like '%data scien%' then 'Data Science'
            when lower(job_title) like '%data analy%' then 'Data Analytics'
            when lower(job_title) like '%software engineer%' or lower(job_title) like '%developer%' then 'Software Development'
            when lower(job_title) like '%devops%' then 'DevOps'
            when lower(job_title) like '%machine learning%' or lower(job_title) like '%ml %' or lower(job_title) like '%ai %' then 'AI/ML'
            when lower(job_title) like '%frontend%' or lower(job_title) like '%front-end%' then 'Frontend Development'
            when lower(job_title) like '%backend%' or lower(job_title) like '%back-end%' then 'Backend Development'
            when lower(job_title) like '%full stack%' or lower(job_title) like '%fullstack%' then 'Full Stack Development'
            else 'Other Tech'
        end as tech_skill_category,
        
        -- Infer cloud platforms from job titles
        case 
            when lower(job_title) like '%aws%' or lower(job_title) like '%amazon%' then 'AWS'
            when lower(job_title) like '%azure%' then 'Azure'
            when lower(job_title) like '%gcp%' or lower(job_title) like '%google cloud%' then 'GCP'
            else 'Cloud Agnostic'
        end as cloud_platform_focus,
        
        -- Programming language inference (basic)
        case
            when lower(job_title) like '%python%' then 'Python'
            when lower(job_title) like '%java%' and not lower(job_title) like '%javascript%' then 'Java'
            when lower(job_title) like '%javascript%' or lower(job_title) like '%react%' or lower(job_title) like '%node%' then 'JavaScript'
            when lower(job_title) like '%c#%' or lower(job_title) like '%.net%' then 'C#/.NET'
            when lower(job_title) like '%c++%' then 'C++'
            else 'Not Specified'
        end as primary_language_hint
        
    from {{ ref('stg_job_postings') }}
    where job_function in ('Information Technology', 'Engineering', 'Engineering and Information Technology')
),

demand_analytics as (
    select
        tech_skill_category,
        cloud_platform_focus,
        primary_language_hint,
        job_seniority_level,
        job_state,
        
        -- Demand metrics
        count(*) as total_demand,
        count(case when job_posted_date >= current_date - interval 30 days then 1 end) as recent_demand,
        count(distinct company_name) as companies_hiring,
        
        -- Market dynamics
        avg(case when max_salary > 0 then max_salary end) as avg_max_salary,
        count(case when job_employment_type = 'Contract' then 1 end) as contract_demand,
        
        -- Growth indicators
        count(case when job_posted_date >= current_date - interval 7 days then 1 end) as weekly_postings,
        
        -- Calculate momentum (recent vs historical demand)
        round(
            count(case when job_posted_date >= current_date - interval 30 days then 1 end) * 4.0 /
            nullif(count(case when job_posted_date >= current_date - interval 120 days then 1 end), 0)
        , 2) as demand_momentum_ratio
        
    from tech_role_classification
    where tech_skill_category != 'Other Tech'
    group by 1,2,3,4,5
),

market_insights as (
    select *,
        -- Demand classification
        case
            when recent_demand >= 10 then 'HIGH_DEMAND'
            when recent_demand >= 5 then 'MODERATE_DEMAND'
            when recent_demand >= 2 then 'LOW_DEMAND'
            else 'MINIMAL_DEMAND'
        end as demand_level,
        
        -- Market trend
        case
            when demand_momentum_ratio >= 1.5 then 'RAPIDLY_GROWING'
            when demand_momentum_ratio >= 1.2 then 'GROWING'
            when demand_momentum_ratio >= 0.8 then 'STABLE'
            else 'DECLINING'
        end as market_trend,
        
        -- Opportunity score (0-100)
        least(100, round(
            (recent_demand * 5) + 
            (companies_hiring * 3) + 
            (case when avg_max_salary > 100000 then 20 else avg_max_salary/5000 end) +
            (demand_momentum_ratio * 10)
        , 0)) as opportunity_score
        
    from demand_analytics
)

select * from market_insights
order by opportunity_score desc
