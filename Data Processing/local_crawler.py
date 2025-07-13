import json
import random

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
NUMBER_OF_PAGES=3
LISTED_AT=259200
SEARCH_LIMIT=10
SEARCH_LOCATION="Australia"
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
            print("request failed: {}".format(data["message"]))
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

# Set up logging


def search_jobs(api: Linkedin, keywords: str, offset: int) -> List[Dict]:
    """Search for jobs using LinkedIn API"""
    return api.search_jobs(
        keywords='Data Engineer',
        location_name=SEARCH_LOCATION,
        limit=SEARCH_LIMIT,
        listed_at=LISTED_AT,
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

       
        return job_info

    except Exception as e:
        print(f"Error processing job {job_id}: {str(e)}")
        return None

def local_handler():
    all_jobid=set()
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
        cookies1.set('li_at', 'AQEDAVwTQIoDeO7mAAABmANQU2gAAAGYJ1zXaFYAXX8sqInOmdslmuuFxfxUOtCkKnXE_SO5n58Pk4wKSDYhZb0heTffQZpMcPzoUfsiVUvuGEaEwKPKnnZpUDBzUYJec-T-q2FZM9ryJsQ046Vzmiab', domain='www.linkedin.com')
        cookies1.set('JSESSIONID', 'ajax:8814767030682013263', domain='www.linkedin.com')

# Authenticate using the cookies
# Make sure to pass an empty password when using cookies
        api1= Linkedin(cookies=cookies1,username='datajr16001@gmail.com', password='')

        # Initialize LinkedIn clients
       
        print("Account 1 authenticated")
        
        cookies2 = RequestsCookieJar()
        cookies2.set('li_at', 'AQEDAVwTQMIFe0BUAAABmANSJvkAAAGYJ16q-VYAJ3bX0T_-lBAtWKA8LXyzIbKjF14VDsm4tfoqUdzGpCcQkE7NZwjnfF1ZMCO6ohtf4LRreyZbYR9Xa-v-qIjtsO_6yQtvJ7KNB5OF5kuUa0kc1D46', domain='www.linkedin.com')
        cookies2.set('JSESSIONID', 'ajax:4512480560839978586', domain='www.linkedin.com')
        api2= Linkedin(cookies=cookies2,username='datajr16002@gmail.com', password='')
# Authenticate using the cookies
# Make sure to pass an empty password when using cookies
        print("Account 2 authenticated")

        # Initialize LinkedIn clients

        # Main processing
        all_jobs = []
        seen_in_current_search = set()  # used for deduplication during pagination
        
        print("Starting job search...")
        
        for page in range (NUMBER_OF_PAGES) :
            offset = page * SEARCH_LIMIT
            print(f"Searching page {page + 1} (offset {offset})...")
            
            ##jobs = search_jobs(api1, keyword, offset)
            try:
                jobs = search_jobs(api1, keyword, offset)
                print(f"Found {len(jobs)} jobs on page {page + 1}")
                # Log redirect information from the last request
                if hasattr(api1, 'client') and hasattr(api1.client, 'session'):
                    last_response = getattr(api1.client.session, 'last_response', None)
                    if last_response:
                        print(f"Final URL after redirects: {last_response.url}")
                        print(f"Response status code: {last_response.status_code}")
                        print(f"Number of redirects followed: {len(last_response.history)}")
                        # Log each redirect in the chain
                        for i, redirect in enumerate(last_response.history):
                            print(f"Redirect {i+1}: {redirect.status_code} {redirect.url} -> {redirect.headers.get('Location', 'N/A')}")
                        # Log response headers that might indicate why we're being redirected
                        important_headers = ['Location', 'Set-Cookie', 'X-Li-Error-Code', 'X-LI-UUID', 'X-RestLi-Error-Response']
                        for header in important_headers:
                            if header in last_response.headers:
                                print(f"Response header {header}: {last_response.headers[header]}")
            
            except Exception as e:
                print(f"Failed to search jobs on page {page + 1}: {str(e)}")
                # Log redirect info even when there's an error
                if hasattr(api1, 'client') and hasattr(api1.client, 'session'):
                    try:
                        # Get the last response even if it failed
                        last_response = api1.client.session.get_adapter('https://').last_response if hasattr(api1.client.session.get_adapter('https://'), 'last_response') else None
                        if last_response:
                            print(f"Error occurred at URL: {last_response.url}")
                            print(f"Redirect chain length: {len(last_response.history)}")
                            for i, redirect in enumerate(last_response.history):
                                print(f"Redirect {i+1}: {redirect.status_code} from {redirect.url}")
                    except:
                        pass
                continue
            print(f"Found {len(jobs)} jobs on page {page + 1}")
            
            for job in jobs:
                result = process_job(job, api2)
                all_jobid.add(result['job_id'])
                if result is not None:
                    job_info = result
                    # check scraping time window to avoid duplication caused by newly posted jobs during running time
                    '''if not (past_time <= int(job_info['posted_time']/1000) <= current_time):
                        logger.info(f"Job {job_info['job_id']} posted time {job_info['posted_time']} is outside window {past_time} - {current_time}")
                        continue'''
                    # avoid duplication caused by "existing jobs get pushed to later pages when new job postings during pagination"
                    if job_info['job_id'] in seen_in_current_search:
                        print(f"Skipping duplicate job {job_info['job_id']} in current search")
                        continue
                    seen_in_current_search.add(job_info['job_id'])
                    all_jobs.append(job_info)

            if len(jobs) < int(SEARCH_LIMIT):
                print(f"Found {len(jobs)} jobs which is less than limit {int(SEARCH_LIMIT)}, stopping search...")
                break

        # Convert to DataFrame and save as Parquet
        if all_jobs:
            df = pd.DataFrame(all_jobs)
            
            # Convert to proper data types
            df['posted_time'] = pd.to_datetime(df['posted_time'], unit='ms', errors='coerce')
            df['expire_time'] = pd.to_datetime(df['expire_time'], unit='ms', errors='coerce')
            df['reposted'] = df['reposted'].astype(bool)
            
         
            parquet_buffer = io.BytesIO()
            df.to_parquet('sample.parquet',
              compression='snappy')
            df1=pd.DataFrame({job_id:all_jobid})
            df1.to_csv('out.csv', index=False)    
        
    

        
        return 1
        
    except Exception as e:
        print(f"Error in lambda function: {str(e)}")
        raise







print(local_handler())