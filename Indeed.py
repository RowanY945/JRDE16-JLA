import pandas as pd
from jobspy import scrape_jobs
import time
from datetime import datetime
import logging
import uuid

# Log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def is_job_remote(job: dict, description: str) -> bool:
    """
    Searches the description, location, and attributes to check if job is remote
    """
    remote_keywords = ["remote", "work from home", "wfh"]

    attributes = job.get("attributes", []) if isinstance(job, dict) else []
    if not isinstance(attributes, list):
        logger.debug(f"Attributes is not a list: {attributes}")
        attributes = []

    is_remote_in_attributes = any(
        any(keyword in attr.get("label", "").lower() for keyword in remote_keywords)
        for attr in attributes
    )

    is_remote_in_description = any(
        keyword in description.lower() for keyword in remote_keywords
    ) if isinstance(description, str) else False

    location = job.get("location", {}) if isinstance(job, dict) else {}
    location_long = (
        location.get("formatted", {}).get("long", "").lower()
        if isinstance(location, dict)
        else ""
    )
    is_remote_in_location = any(
        keyword in location_long for keyword in remote_keywords
    )

    return is_remote_in_attributes or is_remote_in_description or is_remote_in_location

def is_job_hybrid(job: dict, description: str) -> bool:
    """
    Searches the description, location, and attributes to check if job is hybrid
    """
    hybrid_keywords = ["hybrid", "partially remote", "flexible work", "mixed work"]

    attributes = job.get("attributes", []) if isinstance(job, dict) else []
    if not isinstance(attributes, list):
        logger.debug(f"Attributes is not a list: {attributes}")
        attributes = []

    is_hybrid_in_attributes = any(
        any(keyword in attr.get("label", "").lower() for keyword in hybrid_keywords)
        for attr in attributes
    )

    is_hybrid_in_description = any(
        keyword in description.lower() for keyword in hybrid_keywords
    ) if isinstance(description, str) else False

    location = job.get("location", {}) if isinstance(job, dict) else {}
    location_long = (
        location.get("formatted", {}).get("long", "").lower()
        if isinstance(location, dict)
        else ""
    )
    is_hybrid_in_location = any(
        keyword in location_long for keyword in hybrid_keywords
    )

    return is_hybrid_in_attributes or is_hybrid_in_description or is_hybrid_in_location

def infer_job_functions(description):
    """
    Infer job functions from the job description if job_function is missing.
    """
    if not isinstance(description, str):
        return 'N/A'

    description = description.lower()
    functions = []
    if 'engineer' in description or 'engineering' in description:
        functions.append('Engineering')
    if 'data' in description or 'analytics' in description or 'analysis' in description:
        functions.append('Data Analysis')
    if 'develop' in description or 'development' in description or 'developer' in description:
        functions.append('Software Development')
    if 'management' in description or 'manager' in description:
        functions.append('Project Management')
    if 'ai' in description or 'machine learning' in description or 'artificial intelligence' in description:
        functions.append('AI/ML')

    return ', '.join(functions) if functions else 'N/A'

def infer_seniority_level(title):
    """
    Infer seniority level from the job title.
    """
    if not isinstance(title, str):
        return ''

    title = title.lower()
    if 'senior' in title or 'sr' in title:
        return 'Senior'
    elif 'lead' in title or 'principal' in title:
        return 'Lead'
    elif 'junior' in title or 'jr' in title:
        return 'Junior'
    elif 'mid' in title or 'intermediate' in title:
        return 'Mid-level'
    else:
        return ''

def infer_skills(description):
    """
    Infer skills from the job description.
    """
    if not isinstance(description, str):
        return 'N/A'

    description = description.lower()
    skills_list = [
        'python', 'sql', 'java', 'scala', 'r', 'c++', 'javascript',
        'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'spark', 'hadoop',
        'kafka', 'airflow', 'snowflake', 'databricks', 'redshift', 'bigquery',
        'etl', 'data pipeline', 'data modeling', 'database', 'tableau', 'power bi',
        'communication', 'teamwork', 'problem-solving', 'leadership', 'collaboration',
        'project management', 'analytical skills'
    ]
    found_skills = [skill for skill in skills_list if skill in description]

    return ', '.join(found_skills) if found_skills else 'N/A'

def scrape_data_engineer_jobs_simple(search_term="Data Engineer", location="Melbourne, VIC, Australia",
                                     results_wanted=50):
    """
    Simplified data engineer job scraper
    """
    logger.info(f"Scraping: {search_term} in {location}")

    try:
        jobs = scrape_jobs(
            site_name=['indeed'],
            search_term=search_term,
            location=location,
            results_wanted=results_wanted,
            hours_old=168,
            country_indeed='Australia',
            linkedin_fetch_description=True,
        )

        if jobs is not None and not jobs.empty:
            logger.debug(f"Sample job row: {jobs.iloc[0].to_dict()}")

            jobs = jobs.rename(columns={
                'id': 'job_id',
                'job_type': 'employment_type',
                'company_industry': 'industries',
                'is_remote': 'workplace_type',
                'date_posted': 'posted_time',
                'job_url': 'apply_url'
            })

            def determine_workplace_type(row):
                job_dict = row.to_dict()
                description = row.get('description', '')
                if not isinstance(description, str):
                    description = ''
                try:
                    if is_job_remote(job_dict, description):
                        return 'Remote'
                    elif is_job_hybrid(job_dict, description):
                        return 'Hybrid'
                    else:
                        return 'On-site'
                except Exception as e:
                    logger.error(f"Error determining workplace_type for job {row.get('job_id', 'unknown')}: {str(e)}")
                    return 'On-site'

            jobs['workplace_type'] = jobs.apply(determine_workplace_type, axis=1)

            jobs['job_functions'] = jobs.apply(
                lambda row: row.get('job_function', 'N/A') if row.get('job_function') else infer_job_functions(
                    row.get('description', '')),
                axis=1
            )
            jobs['seniority_level'] = jobs['title'].apply(infer_seniority_level)
            jobs['skills'] = jobs['description'].apply(infer_skills)
            jobs['reposted'] = 'N/A'
            jobs['posted_time'] = jobs['posted_time'].apply(
                lambda x: pd.to_datetime(x).strftime('%Y-%m-%d %H:%M:%S.%f') if pd.notnull(x) else 'N/A'
            )
            jobs['title'] = jobs['title'].fillna('N/A')
            jobs['company'] = jobs['company'].fillna('N/A')
            jobs['location'] = jobs['location'].fillna('N/A')
            jobs['description'] = jobs['description'].fillna('N/A')
            jobs['apply_url'] = jobs['apply_url'].fillna('N/A')

            columns_to_keep = [
                'job_id', 'title', 'company', 'location', 'employment_type',
                'seniority_level', 'industries', 'job_functions', 'workplace_type',
                'description', 'skills', 'apply_url', 'reposted', 'posted_time'
            ]
            jobs = jobs[columns_to_keep]

            logger.info(f"Successfully scraped {len(jobs)} jobs")
            return jobs
        else:
            logger.warning("No jobs scraped")
            return pd.DataFrame(columns=[
                'job_id', 'title', 'company', 'location', 'employment_type',
                'seniority_level', 'industries', 'job_functions', 'workplace_type',
                'description', 'skills', 'apply_url', 'reposted', 'posted_time'
            ])

    except Exception as e:
        logger.error(f"Scraping error: {str(e)}")
        return pd.DataFrame(columns=[
            'job_id', 'title', 'company', 'location', 'employment_type',
            'seniority_level', 'industries', 'job_functions', 'workplace_type',
            'description', 'skills', 'apply_url', 'reposted', 'posted_time'
        ])

def scrape_multiple_locations(search_terms=None, locations=None, results_per_search=30):
    """
    Scrape jobs for multiple locations and search terms
    """
    if search_terms is None:
        search_terms = ["Data Engineer", "Senior Data Engineer"]

    if locations is None:
        locations = [
            "Melbourne, VIC, Australia",
            "Sydney, NSW, Australia",
            "Brisbane, QLD, Australia"
        ]

    all_jobs = []

    for location in locations:
        for search_term in search_terms:
            logger.info(f"Searching: {search_term} @ {location}")

            jobs = scrape_data_engineer_jobs_simple(
                search_term=search_term,
                location=location,
                results_wanted=results_per_search
            )

            if not jobs.empty:
                all_jobs.append(jobs)

            time.sleep(3)

    if all_jobs:
        final_df = pd.concat(all_jobs, ignore_index=True)
        if 'apply_url' in final_df.columns:
            final_df = final_df.drop_duplicates(subset=['apply_url'], keep='first')
        else:
            final_df = final_df.drop_duplicates(subset=['title', 'company'], keep='first')

        logger.info(f"Total unique jobs scraped: {len(final_df)}")
        return final_df
    else:
        logger.warning("No jobs scraped")
        return pd.DataFrame(columns=[
            'job_id', 'title', 'company', 'location', 'employment_type',
            'seniority_level', 'industries', 'job_functions', 'workplace_type',
            'description', 'skills', 'apply_url', 'reposted', 'posted_time'
        ])

def save_raw_data(df, filename_prefix="au_data_engineer_jobs"):
    """
    Save raw data to files
    """
    if df.empty:
        logger.warning("No data to save")
        return

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    csv_filename = f"{filename_prefix}_{timestamp}.csv"
    df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
    logger.info(f"Raw data saved to {csv_filename}")

    excel_filename = f"{filename_prefix}_{timestamp}.xlsx"
    df.to_excel(excel_filename, index=False, engine='openpyxl')
    logger.info(f"Raw data saved to {excel_filename}")

    print(f"\nData Summary:")
    print(f"Total jobs: {len(df)}")
    print(f"Number of columns: {len(df.columns)}")
    print(f"Columns: {list(df.columns)}")

def main():
    """
    Main function - runs full multi-location scrape
    """
    print("Australian Data Engineer Job Scraper")
    print("=" * 50)
    print("Starting full multi-location scrape...")

    df = scrape_multiple_locations()

    if not df.empty:
        save_raw_data(df, "multi_location_jobs")

        print("\nSample job (first row):")
        print(f"Job ID: {df.iloc[0].get('job_id', 'N/A')}")
        print(f"Title: {df.iloc[0].get('title', 'N/A')}")
        print(f"Company: {df.iloc[0].get('company', 'N/A')}")
        print(f"Location: {df.iloc[0].get('location', 'N/A')}")
        print(f"Employment Type: {df.iloc[0].get('employment_type', 'N/A')}")
        print(f"Seniority Level: {df.iloc[0].get('seniority_level', 'N/A')}")
        print(f"Industries: {df.iloc[0].get('industries', 'N/A')}")
        print(f"Job Functions: {df.iloc[0].get('job_functions', 'N/A')}")
        print(f"Workplace Type: {df.iloc[0].get('workplace_type', 'N/A')}")
        print(f"Description: {df.iloc[0].get('description', 'N/A')[:100]}...")
        print(f"Skills: {df.iloc[0].get('skills', 'N/A')}")
        print(f"Apply URL: {df.iloc[0].get('apply_url', 'N/A')}")
        print(f"Reposted: {df.iloc[0].get('reposted', 'N/A')}")
        print(f"Posted Time: {df.iloc[0].get('posted_time', 'N/A')}")
    else:
        print("No jobs found")

if __name__ == "__main__":
    print("Ensure dependencies are installed:")
    print("pip install python-jobspy pandas openpyxl")
    print()
    main()
