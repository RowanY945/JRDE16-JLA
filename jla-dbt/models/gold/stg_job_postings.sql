with source_data as (
    select * from `demo`.`gold`.`stg_job_postings`
),

salary_data as (
    select
        job_title,
        job_seniority_level,
        job_function,
        job_industries,
        job_city,
        job_state,
        min_salary,
        max_salary,
        (min_salary + max_salary) / 2.0 as mid_salary,
        job_employment_type,
        company_name
    from source_data
    where min_salary is not null and max_salary is not null
),

salary_benchmarks as (
    select
        job_title,
        job_seniority_level,
        job_function,
        job_state,
        count(*) as sample_size,
        round(avg(mid_salary), 0) as avg_salary,
        round(percentile_approx(mid_salary, 0.25), 0) as p25_salary,
        round(percentile_approx(mid_salary, 0.5), 0) as median_salary,  
        round(percentile_approx(mid_salary, 0.75), 0) as p75_salary,
        round(min(mid_salary), 0) as min_salary_observed,
        round(max(mid_salary), 0) as max_salary_observed,
        round(avg(max_salary - min_salary), 0) as avg_salary_range_width,
        round(stddev(mid_salary), 0) as salary_std_dev,
        case 
            when avg(mid_salary) >= 150000 then 'PREMIUM'
            when avg(mid_salary) >= 100000 then 'COMPETITIVE'
            when avg(mid_salary) >= 70000 then 'STANDARD'
            else 'ENTRY_LEVEL'
        end as market_tier,
        round((max(mid_salary) - min(mid_salary)) / nullif(min(mid_salary), 0) * 100, 1) as growth_potential_pct
    from salary_data
    group by 1,2,3,4
    having count(*) >= 3
)

select * from salary_benchmarks
