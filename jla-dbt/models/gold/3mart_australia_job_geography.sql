
{{ 
    config(
        materialized='table',
        file_format='delta',
        location_root='s3://jla-data-gold/',
        tags=['analytics', 'geography', 'australia']
    ) 
}}


with source_data as (
    select * from {{ source('gold_layer', 'stg_job_postings') }}
),

city_coordinates as (
    -- Australian major cities with approximate coordinates
    select * from values
        ('Sydney', 'NSW', -33.8688, 151.2093, 'Major'),
        ('Melbourne', 'VIC', -37.8136, 144.9631, 'Major'),
        ('Brisbane', 'QLD', -27.4698, 153.0251, 'Major'),
        ('Perth', 'WA', -31.9505, 115.8605, 'Major'),
        ('Adelaide', 'SA', -34.9285, 138.6007, 'Major'),
        ('Canberra', 'ACT', -35.2809, 149.1300, 'Major'),
        ('Darwin', 'NT', -12.4634, 130.8456, 'Major'),
        ('Hobart', 'TAS', -42.8821, 147.3272, 'Major'),
        ('Gold Coast', 'QLD', -28.0167, 153.4000, 'Secondary'),
        ('Newcastle', 'NSW', -32.9283, 151.7817, 'Secondary'),
        ('Wollongong', 'NSW', -34.4278, 150.8931, 'Secondary'),
        ('Sunshine Coast', 'QLD', -26.6500, 153.0667, 'Secondary'),
        ('Geelong', 'VIC', -38.1499, 144.3617, 'Secondary'),
        ('Townsville', 'QLD', -19.2590, 146.8169, 'Secondary'),
        ('Cairns', 'QLD', -16.9186, 145.7781, 'Secondary'),
        ('Toowoomba', 'QLD', -27.5598, 151.9507, 'Secondary'),
        ('Ballarat', 'VIC', -37.5622, 143.8503, 'Secondary'),
        ('Bendigo', 'VIC', -36.7570, 144.2794, 'Secondary'),
        ('Albury', 'NSW', -36.0737, 146.9135, 'Secondary'),
        ('Launceston', 'TAS', -41.4332, 147.1441, 'Secondary'),
        ('Mackay', 'QLD', -21.1550, 149.1613, 'Secondary'),
        ('Rockhampton', 'QLD', -23.3781, 150.5069, 'Secondary'),
        ('Bunbury', 'WA', -33.3267, 115.6411, 'Secondary'),
        ('Bundaberg', 'QLD', -24.8661, 152.3489, 'Secondary'),
        ('Wagga Wagga', 'NSW', -35.1082, 147.3598, 'Secondary'),
        -- Sydney suburbs
        ('North Sydney', 'NSW', -33.8403, 151.2065, 'Suburb'),
        ('Surry Hills', 'NSW', -33.8886, 151.2094, 'Suburb'),
        ('Parramatta', 'NSW', -33.8150, 151.0010, 'Suburb'),
        ('Baulkham Hills', 'NSW', -33.7581, 150.9876, 'Suburb'),
        ('Dee Why', 'NSW', -33.7581, 151.2886, 'Suburb'),
        ('Millers Point', 'NSW', -33.8569, 151.2055, 'Suburb'),
        -- Melbourne suburbs  
        ('Glen Waverley', 'VIC', -37.8770, 145.1608, 'Suburb'),
        ('Mulgrave', 'VIC', -37.9262, 145.1431, 'Suburb'),
        ('Footscray', 'VIC', -37.7993, 144.9009, 'Suburb'),
        ('Bundoora', 'VIC', -37.7004, 145.0581, 'Suburb'),
        -- Other locations from your data
        ('Virginia', 'SA', -34.6667, 138.5500, 'Regional'),
        ('No Regrets', 'QLD', -27.4698, 153.0251, 'Special') -- Appears to be Brisbane-based
    as t(city_name, state_code, latitude, longitude, city_tier)
),

job_geographic_data as (
    select
        j.job_posting_id,
        j.job_title,
        j.company_name,
        j.job_city,
        j.job_state,
        j.job_seniority_level,
        j.job_function,
        j.job_industries,
        j.job_employment_type,
        j.job_posted_date,
        j.min_salary,
        j.max_salary,
        coalesce(j.min_salary + j.max_salary) / 2.0 as mid_salary,
        
        -- Geographic mapping
        coalesce(c.latitude, 
            case j.job_state
                when 'NSW' then -33.8688
                when 'VIC' then -37.8136  
                when 'QLD' then -27.4698
                when 'WA' then -31.9505
                when 'SA' then -34.9285
                when 'ACT' then -35.2809
                when 'NT' then -12.4634
                when 'TAS' then -42.8821
                else -25.2744 -- Australia center
            end) as latitude,
            
        coalesce(c.longitude,
            case j.job_state  
                when 'NSW' then 151.2093
                when 'VIC' then 144.9631
                when 'QLD' then 153.0251
                when 'WA' then 115.8605
                when 'SA' then 138.6007
                when 'ACT' then 149.1300
                when 'NT' then 130.8456
                when 'TAS' then 147.3272
                else 133.7751 -- Australia center
            end) as longitude,
            
        coalesce(c.city_tier, 'Unknown') as city_tier,
        
        -- Distance from major city (Sydney as reference)
        round(6371 * acos(cos(radians(-33.8688)) 
            * cos(radians(coalesce(c.latitude, -33.8688))) 
            * cos(radians(coalesce(c.longitude, 151.2093)) - radians(151.2093)) 
            + sin(radians(-33.8688)) 
            * sin(radians(coalesce(c.latitude, -33.8688)))), 0) as distance_from_sydney_km
        
    from {{ ref('stg_job_postings') }} j
    left join city_coordinates c 
        on lower(trim(j.job_city)) = lower(c.city_name) 
        and j.job_state = c.state_code
),

geographic_aggregations as (
    select
        -- Geographic identifiers
        job_state as state,
        job_city as city,
        latitude,
        longitude,
        city_tier,
        
        -- Job market metrics
        count(*) as total_jobs,
        count(distinct company_name) as unique_companies,
        count(distinct job_function) as function_diversity,
        count(distinct job_industries) as industry_diversity,
        
        -- Employment metrics
        sum(case when job_employment_type = 'Full-time' then 1 else 0 end) as fulltime_jobs,
        sum(case when job_employment_type = 'Contract' then 1 else 0 end) as contract_jobs,
        sum(case when job_employment_type = 'Part-time' then 1 else 0 end) as parttime_jobs,
        
        -- Seniority breakdown
        sum(case when job_seniority_level in ('Entry level', 'Associate') then 1 else 0 end) as entry_level_jobs,
        sum(case when job_seniority_level = 'Mid-Senior level' then 1 else 0 end) as midsenior_jobs,
        sum(case when job_seniority_level in ('Director', 'Executive') then 1 else 0 end) as senior_jobs,
        
        -- Salary analysis
        avg(case when mid_salary > 0 then mid_salary end) as avg_salary,
        min(case when mid_salary > 0 then mid_salary end) as min_salary_market,
        max(case when mid_salary > 0 then mid_salary end) as max_salary_market,
        count(case when mid_salary > 0 then 1 end) as salary_sample_size,
        
        -- Temporal metrics
        count(case when job_posted_date >= current_date - interval 7 days then 1 end) as jobs_last_week,
        count(case when job_posted_date >= current_date - interval 30 days then 1 end) as jobs_last_month,
        
        -- Geographic context
        avg(distance_from_sydney_km) as avg_distance_from_sydney,
        
        -- Top industries and functions (JSON aggregation)
        array_agg(distinct job_industries) as top_industries,
        array_agg(distinct job_function) as top_functions
        
    from job_geographic_data
    where latitude is not null and longitude is not null
    group by 1,2,3,4,5
),

market_classification as (
    select *,
        -- Market size classification
        case
            when total_jobs >= 50 then 'MAJOR_MARKET'
            when total_jobs >= 20 then 'SIGNIFICANT_MARKET'
            when total_jobs >= 10 then 'EMERGING_MARKET'
            when total_jobs >= 5 then 'SMALL_MARKET'
            else 'MINIMAL_MARKET'
        end as market_size,
        
        -- Market activity level
        case
            when jobs_last_week >= 5 then 'VERY_ACTIVE'
            when jobs_last_month >= 10 then 'ACTIVE'
            when jobs_last_month >= 3 then 'MODERATE'
            else 'QUIET'
        end as activity_level,
        
        -- Salary competitiveness (compared to national average)
        case
            when avg_salary >= 120000 then 'HIGH_SALARY'
            when avg_salary >= 90000 then 'COMPETITIVE_SALARY'
            when avg_salary >= 70000 then 'AVERAGE_SALARY'
            when avg_salary > 0 then 'BELOW_AVERAGE_SALARY'
            else 'SALARY_UNKNOWN'
        end as salary_tier,
        
        -- Market diversity score (0-100)
        least(100, round(
            (function_diversity * 10) + 
            (industry_diversity * 8) + 
            (unique_companies * 3) +
            (case when city_tier = 'Major' then 20 when city_tier = 'Secondary' then 10 else 5 end)
        , 0)) as market_diversity_score,
        
        -- Employment flexibility score
        round(((contract_jobs + parttime_jobs) * 100.0 / total_jobs), 1) as flexibility_ratio,
        
        -- Market opportunity index
        round(ln(total_jobs + 1) * ln(unique_companies + 1) * 
              (case when avg_salary > 50000 then ln(avg_salary/50000 + 1) else 1 end) *
              (1 + jobs_last_month/10.0), 2) as opportunity_index
        
    from geographic_aggregations
)

select 
    -- Geographic info
    state,
    city,
    latitude,
    longitude,
    city_tier,
    
    -- Market metrics
    total_jobs,
    unique_companies,
    function_diversity,
    industry_diversity,
    market_size,
    activity_level,
    
    -- Employment breakdown
    fulltime_jobs,
    contract_jobs,
    parttime_jobs,
    flexibility_ratio,
    
    -- Seniority distribution
    entry_level_jobs,
    midsenior_jobs, 
    senior_jobs,
    
    -- Salary intelligence
    avg_salary,
    min_salary_market,
    max_salary_market,
    salary_sample_size,
    salary_tier,
    
    -- Recent activity
    jobs_last_week,
    jobs_last_month,
    
    -- Market scores
    market_diversity_score,
    opportunity_index,
    
    -- Geographic context
    avg_distance_from_sydney,
    
    -- Industry/function arrays
    top_industries,
    top_functions,
    
    -- Metadata
    current_timestamp() as analysis_date
    
from market_classification
order by opportunity_index desc