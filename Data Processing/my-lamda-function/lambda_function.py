import json
import random
import os
import boto3
from datetime import datetime, timezone
import logging
import time
from time import sleep
from open_linkedin_api import Linkedin
from functools import partial
from typing import Dict, List
import pandas as pd
import io
from requests.cookies import RequestsCookieJar
keyword="Data engineer"
def get_j(self, job_id: str):
        """Fetch data about a given job.
        :param job_id: LinkedIn job ID
        :type job_id: str

        :return: Job data
        :rtype: dict
        """
        params = {
            "decorationId": "com.linkedin.voyager.deco.jobs.web.shared.WebFullJobPosting-63",
        }

        res = self._fetch(f"/jobs/jobPostings/{job_id}", params=params)

        data = res.json()

        if data and "status" in data and data["status"] != 200:
            self.logger.info("request failed: {}".format(data["message"]))
            return {}

        return data 

def new_evade():
    """
    A catch-all method to try and evade suspension from Linkedin.
    Currenly, just delays the request by a random (bounded) time
    """
    sleep(random.randint(3, 6))  # sleep a random duration to try and evade suspention

Linkedin.get_job=get_j
Linkedin.default_evade=new_evade
"""
https://github.com/tomquirk/linkedin-api
""" 



# Initialize AWS clients
s3 = boto3.client('s3')

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def search_jobs(api: Linkedin, keywords: str, offset: int) -> List[Dict]:
    """Search for jobs using LinkedIn API"""
    return api.search_jobs(
        keywords='Data Engineer',
        location_name=os.environ['SEARCH_LOCATION'],
        limit=int(os.environ['SEARCH_LIMIT']),
        listed_at=int(os.environ['LISTED_AT']),
        offset=offset 
    )

def get_job_details(api: Linkedin, job_id: str) -> Dict:
    """Get detailed job information"""
    return api.get_job(job_id)

def process_job(job: Dict, api2: Linkedin) -> Dict:
    """Process a single job"""
    try:
        job_id = job.get('id') or job.get('entityUrn', '').split(':')[-1]
        if not job_id:
            logger.error("Job ID not found")
            return None

        # Get job details
        job_details = get_job_details(api2, job_id)

        # Extract company information
        company = (
            job.get('companyName') or
            job_details.get('companyName') or
            job_details.get('companyDetails', {}).get('companyName') or
            job_details.get('companyDetails', {}).get('company', {}).get('name') or
            "N/A"
        )

        # Extract location
        location = (
            job.get('formattedLocation') or
            job_details.get('formattedLocation') or
            job_details.get('locationDescription') or
            job_details.get('location', {}).get('city') or
            "N/A"
        )

        # Listed time & Expire time
        listed_at_epoch = job.get('listedAt') or job_details.get('listedAt')
        expire_at_epoch = job.get('expireAt') or job_details.get('expireAt')

        # Extract Job URL
        job_url = f"https://www.linkedin.com/jobs/view/{job_id}"

        # Extract apply URL
        apply_method = job_details.get("applyMethod", {})
        apply_url = apply_method.get("com.linkedin.voyager.jobs.OffsiteApply", {}).get("companyApplyUrl") or job_url

        # Compile job information
        job_info = {
            "job_id": job_id,
            "title": job.get('title', "N/A"),
            "company": company,
            "location": location,
            "employment_type": job_details.get('formattedEmploymentStatus', "N/A"),
            "seniority_level": job_details.get('formattedExperienceLevel', ""),
            "industries": str(job_details.get('formattedIndustries', "N/A")),
            "job_functions": str(job_details.get('formattedJobFunctions', "N/A")),
            "workplace_type": str(job_details.get('workplaceTypes', [])),
            "description": job_details.get('description', {}).get('text', "N/A"),
            "job_url": job_url,
            "reposted": job.get('repostedJob', False),
            "posted_time": listed_at_epoch,
            "expire_time": expire_at_epoch,
            "apply_url": apply_url
        }

        logger.info(f"Processed job: {job_info['title']} at {job_info['company']}")
        return job_info

    except Exception as e:
        logger.error(f"Error processing job {job_id}: {str(e)}")
        return None

def lambda_handler(event, context):
    """Synchronous Lambda function handler"""
    try:
        current_time = int(time.time())
        past_time = current_time - 86400

        # Extract keyword from event
        ''' keyword = event.get('keyword')
            if not keyword:
            raise ValueError("No keyword provided in event")''' 
        # Set the cookies required for authentication
        cookies1 = RequestsCookieJar()
        cookies1.set('li_at', 'AQEDAVwTQIoAIdyyAAABl6Z1LksAAAGXyoGyS1YAhIbbPXot4jPiKYQjdjfSAEplULBdyr3oJFewEI07VCrqOeCPJztDgkCjGy5D7R1hYJMCrgWPOzc4wdPMi72stsbu7DAy81UQUp43vyaK0IOWPZo-e', domain='www.linkedin.com')
        cookies1.set('JSESSIONID', 'ajax:7967078639394692365', domain='www.linkedin.com')

# Authenticate using the cookies
# Make sure to pass an empty password when using cookies
        api1= Linkedin(cookies=cookies1, username='datajr16001@gmail.com', password='Datajr@16')

        # Initialize LinkedIn clients
       
        logger.info("Account 1 authenticated")
        
        cookies2 = RequestsCookieJar()
        cookies2.set('li_at', 'AQEDAVwTQMIBs-p6AAABl6Z3eHkAAAGXyoP8eVYAvJJm-yU6qULEIWNHBTdsli5n2GnuR8wvNt2z3dm7Njkdh1nmNKlOxiSTqVOj58MLzIUU5P917xjwRMUD76iCs9aU9YiNn_ll-AUhxpUJuZxl2Osl', domain='www.linkedin.com')
        cookies2.set('JSESSIONID', 'ajax:6804652351491069637', domain='www.linkedin.com')
        api2= Linkedin(cookies=cookies2, username='datajr16002@gmail.com', password='Datajr@16')
# Authenticate using the cookies
# Make sure to pass an empty password when using cookies
        logger.info("Account 2 authenticated")

        # Initialize LinkedIn clients

        # Main processing
        all_jobs = []
        seen_in_current_search = set()  # used for deduplication during pagination
        
        logger.info("Starting job search...")
        
        for page in range(int(os.environ['NUMBER_OF_PAGES'])):
            offset = page * int(os.environ['SEARCH_LIMIT'])
            logger.info(f"Searching page {page + 1} (offset {offset})...")
            
            ##jobs = search_jobs(api1, keyword, offset)
            try:
                jobs = search_jobs(api1, keyword, offset)
                logger.info(f"Found {len(jobs)} jobs on page {page + 1}")
                # Log redirect information from the last request
                if hasattr(api1, 'client') and hasattr(api1.client, 'session'):
                    last_response = getattr(api1.client.session, 'last_response', None)
                    if last_response:
                        logger.info(f"Final URL after redirects: {last_response.url}")
                        logger.info(f"Response status code: {last_response.status_code}")
                        logger.info(f"Number of redirects followed: {len(last_response.history)}")
                        # Log each redirect in the chain
                        for i, redirect in enumerate(last_response.history):
                            logger.info(f"Redirect {i+1}: {redirect.status_code} {redirect.url} -> {redirect.headers.get('Location', 'N/A')}")
                        # Log response headers that might indicate why we're being redirected
                        important_headers = ['Location', 'Set-Cookie', 'X-Li-Error-Code', 'X-LI-UUID', 'X-RestLi-Error-Response']
                        for header in important_headers:
                            if header in last_response.headers:
                                logger.info(f"Response header {header}: {last_response.headers[header]}")
            
            except Exception as e:
                logger.error(f"Failed to search jobs on page {page + 1}: {str(e)}")
                # Log redirect info even when there's an error
                if hasattr(api1, 'client') and hasattr(api1.client, 'session'):
                    try:
                        # Get the last response even if it failed
                        last_response = api1.client.session.get_adapter('https://').last_response if hasattr(api1.client.session.get_adapter('https://'), 'last_response') else None
                        if last_response:
                            logger.error(f"Error occurred at URL: {last_response.url}")
                            logger.error(f"Redirect chain length: {len(last_response.history)}")
                            for i, redirect in enumerate(last_response.history):
                                logger.error(f"Redirect {i+1}: {redirect.status_code} from {redirect.url}")
                    except:
                        pass
                continue
            logger.info(f"Found {len(jobs)} jobs on page {page + 1}")
            
            for job in jobs:
                result = process_job(job, api2)
                if result is not None:
                    job_info = result
                    # check scraping time window to avoid duplication caused by newly posted jobs during running time
                    '''if not (past_time <= int(job_info['posted_time']/1000) <= current_time):
                        logger.info(f"Job {job_info['job_id']} posted time {job_info['posted_time']} is outside window {past_time} - {current_time}")
                        continue'''
                    # avoid duplication caused by "existing jobs get pushed to later pages when new job postings during pagination"
                    if job_info['job_id'] in seen_in_current_search:
                        logger.info(f"Skipping duplicate job {job_info['job_id']} in current search")
                        continue
                    seen_in_current_search.add(job_info['job_id'])
                    all_jobs.append(job_info)

            if len(jobs) < int(os.environ['SEARCH_LIMIT']):
                logger.info(f"Found {len(jobs)} jobs which is less than limit {int(os.environ['SEARCH_LIMIT'])}, stopping search...")
                break

        # Convert to DataFrame and save as Parquet
        if all_jobs:
            df = pd.DataFrame(all_jobs)
            
            # Convert to proper data types
            df['posted_time'] = pd.to_datetime(df['posted_time'], unit='ms', errors='coerce')
            df['expire_time'] = pd.to_datetime(df['expire_time'], unit='ms', errors='coerce')
            df['reposted'] = df['reposted'].astype(bool)
            
            # Save to Parquet format
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
            parquet_buffer.seek(0)
            
            # Save results to S3
            current_date = datetime.now(timezone.utc)
            year = current_date.strftime('%Y')
            month = current_date.strftime('%m')
            day = current_date.strftime('%d')
            keyword_clean = "Data_Engineer"#keyword.replace(' ', '_')
            jobs_base_key = f"{year}/{month}/{day}/{keyword_clean}"
            file_name = f"{keyword_clean}-{year}{month}{day}.parquet"

            # Save jobs parquet
            s3.put_object(
                Bucket=os.environ['LINKEDIN_DATALAKE_KEY'],
                Key=f"raw/{jobs_base_key}/{file_name}",
                Body=parquet_buffer.getvalue()
            )
            logger.info(f"Job data saved to S3: s3://{os.environ['LINKEDIN_DATALAKE_KEY']}/raw/{jobs_base_key}/")

        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully processed jobs',
                'jobs_processed': len(all_jobs),
                'datalake_path': f"s3://{os.environ['LINKEDIN_DATALAKE_KEY']}/raw/{jobs_base_key}/" if all_jobs else "No jobs to save"
            })
        }
        
    except Exception as e:
        logger.error(f"Error in lambda function: {str(e)}")
        raise

if __name__ == "__main__":
    lambda_handler(None, None)



    cookies = RequestsCookieJar()

