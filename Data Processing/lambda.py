import asyncio
import pickle
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
import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import io
import csv

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
    sleep(random.randint(2, 4))  # sleep a random duration to try and evade suspention

Linkedin.get_job=get_j
Linkedin.default_evade=new_evade
"""
https://github.com/tomquirk/linkedin-api
""" 

"""
Lambda Invocation Flow:

1. lambda_handler (Synchronous Entry Point)
   ↓
2. asyncio.run()
   ↓
3. async_lambda_handler (Async Implementation)
     ↓
     Initialize 3 LinkedIn APIs
     ↓
     Search Jobs (api1)
        ↓
        For each job found:
          ├→ Get Job Details (api2)  ─┐ (in parallel)
          └→ Get Job Skills (api3)   ─┘
          ↓
        Process & Combine Results
     ↓
     Save to S3 as Avro
"""
# Define the Avro schema for job data

AVRO_SCHEMA = {
    "type": "record",
    "name": "JobPosting",
    "fields": [
        {"name": "job_id", "type": ["string", "null"]},
        {"name": "title", "type": ["string", "null"]},
        {"name": "company", "type": ["string", "null"]},
        {"name": "location", "type": ["string", "null"]},
        {"name": "employment_type", "type": ["string", "null"]},
        {"name": "seniority_level", "type": ["string", "null"]},
        {"name": "industries", "type": {"type": "array", "items": "string"}},
        {"name": "job_functions", "type": {"type": "array", "items": "string"}},
        {"name": "workplace_type", "type": ["string", "null"]},
        {"name": "description", "type": ["string", "null"]},
        {"name": "job_url", "type": ["string", "null"]},
        {"name": "reposted", "type": ["boolean", "null"]},
        {"name": "posted_time", "type": ["long", "null"]},
        {"name": "expire_time", "type": ["long", "null"]},
        {"name": "apply_url", "type": ["string", "null"]}
    ]
}
# Initialize AWS clients
s3 = boto3.client('s3')


# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def load_cookies(account_key: str) -> Dict:
    """Load cookies from S3"""
    cookie_file = os.environ[f'{account_key}_COOKIE_FILE']
    local_path = f"/tmp/{cookie_file}"
    
    s3.download_file(
        os.environ['LINKEDIN_COOKIES_KEY'],
        cookie_file,
        local_path
    )
    
    with open(local_path, "rb") as f:
        return pickle.load(f)

async def async_lambda_handler(event, context):
    """Async Lambda function handler"""
    try:
        current_time = int(time.time())
        past_time = current_time-86400

        # Extract keyword from event
        keyword = event.get('keyword')
        if not keyword:
            raise ValueError("No keyword provided in event")
        
        # Initialize LinkedIn clients
        api1 = Linkedin(cookies=':AQEDAVwTQIoAIdyyAAABl6Z1LksAAAGXyoGyS1YAhIbbPXot4jPiKYQjdjfSAEplULBdyr3oJFewEI07VCrqOeCPJztDgkCjGy5D7R1hYJMCrgWPOzc4wdPMi72stsbu7DAy81UQUp43vyaK0IOWPZo-', username='datajr16001@gmail.com', password='Datajr@16')
        logger.info("Account 1 authenticated")
        
        api2 = Linkedin(os.environ['ACCOUNT2_EMAIL'], "", 
                       cookies=load_cookies('ACCOUNT2'))
        logger.info("Account 2 authenticated")

        async def search_jobs(api: Linkedin, keywords: str, offset: int) -> List[Dict]:
            """Async wrapper for LinkedIn job search"""
            search_func = partial(api.search_jobs, 
                                keywords=keywords,
                                location_name=os.environ['SEARCH_LOCATION'],
                                limit=int(os.environ['SEARCH_LIMIT']),
                                listed_at=int(os.environ['LISTED_AT']),
                                offset=offset 
                                )
            return await asyncio.get_event_loop().run_in_executor(None, search_func)

        async def get_job_details(api: Linkedin, job_id: str) -> Dict:
            """Async wrapper for getting job details"""
            return await asyncio.get_event_loop().run_in_executor(
                None, api.get_job, job_id
            )

     

        async def process_job(job: Dict) -> Dict:
            """Process a single job using parallel API calls"""
            try:

                job_id = job.get('id') or job.get('entityUrn', '').split(':')[-1]
                if not job_id:
                    logger.error("Job ID not found")
                    return None

                # Run job details and skills API calls in parallel using different accounts
                job_details_task = get_job_details(api2, job_id)
    
                
                # Wait for both API calls to complete
                job_details = await asyncio.gather(job_details_task)

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

                # Extract skills

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
                    "industries": job_details.get('formattedIndustries', "N/A"),
                    "job_functions": job_details.get('formattedJobFunctions', "N/A"),
                    "workplace_type": job_details.get('workplaceTypes', []),
                    "description": job_details.get('description', {}).get('text', "N/A"),
                    "job_url": job_url,
                    "reposted": job.get('repostedJob', "N/A"),
                    "posted_time": listed_at_epoch,
                    "expire_time": expire_at_epoch,
                    "apply_url": apply_url
                }


                logger.info(f"Processed job: {job_info['title']} at {job_info['company']}")
                return job_info

            except Exception as e:
                logger.error(f"Error processing job {job_id}: {str(e)}")
                return None

        # Main processing
        all_jobs = []
        seen_in_current_search = set() # used for deduplication during pagination
        
        logger.info("Starting job search...")
        
        for page in range(int(os.environ['NUMBER_OF_PAGES'])):
            offset = page * int(os.environ['SEARCH_LIMIT'])
            logger.info(f"Searching page {page + 1} (offset {offset})...")
            
            jobs = await search_jobs(api1, keyword, offset)
            logger.info(f"Found {len(jobs)} jobs on page {page + 1}")
            
            for job in jobs:
                result = await process_job(job)
                if result is not None:
                    job_info = result
                    # check scraping time window to avoid duplication caused by newly posted jobs during running time
                    if not (past_time <= int(job_info['posted_time']/1000) <= current_time):
                        logger.info(f"Job {job_info['job_id']} posted time {job_info['posted_time']} is outside window {past_time} - {current_time}")
                        continue
                    # avoid duplication caused by "existing jobs get pushed to later pages when new job postings during pagination"
                    if job_info['job_id'] in seen_in_current_search:
                        logger.info(f"Skipping duplicate job {job_info['job_id']} in current search")
                        continue
                    seen_in_current_search.add(job_info['job_id'])
                    all_jobs.append(job_info)
                

            if len(jobs) < int(os.environ['SEARCH_LIMIT']):
                logger.info(f"Found {len(jobs)} jobs which is less than limit {int(os.environ['SEARCH_LIMIT'])}, stopping search...")
                break
 
        # Create Avro schema
        schema = avro.schema.parse(json.dumps(AVRO_SCHEMA))
        # Create a bytes IO object to hold the Avro data
        bytes_io = io.BytesIO()
        # Create Avro writer
        try:
            writer = DataFileWriter(bytes_io, DatumWriter(), schema)

            # Write all jobs to Avro
            for job,job_item in all_jobs,jobs:
                # Process job to match Avro schema
                processed_job = {
                    "job_id": str(job.get("job_id", "")) or None,
                    "title": job.get("title", "") or None,
                    "company": job.get("company", "") or None,
                    "location": job.get("location", "") or None,
                    "employment_type": job.get("employment_type", "") or None,
                    "seniority_level": job.get("seniority_level", ""),
                    "industries": job.get("industries", []) or None,
                    "job_functions": job.get("job_functions", []) or None,
                    "workplace_type": job.get("workplace_type", "") or None,
                    "description": job.get("description", "") or None,
                    "job_url": job.get("job_url", "") or None,
                    "reposted": job_item.get("reposted", "") or None,
                    "posted_time": int(job.get("posted_time", 0)) or None,
                    "expire_time": int(job.get("expire_time", 0)) or None,
                    "apply_url": job.get("apply_url", "") or None
                }
                try:
                    writer.append(processed_job)
                except avro.schema.SchemaParseException as e:
                    logger.error(f"Schema validation error for job {job.get('job_id')}: {str(e)}")
                    continue

            writer.flush()
            bytes_io.seek(0)
            avro_data = bytes_io.getvalue()

        finally:
            writer.close()
            bytes_io.close()

        # Save results to S3
        current_date = datetime.now(timezone.utc)
        year = current_date.strftime('%Y')
        month = current_date.strftime('%m')
        day = current_date.strftime('%d')
        keyword = keyword.replace(' ', '_')
        jobs_base_key = f"{year}/{month}/{day}/{keyword}"
        file_name = f"{keyword}-{year}{month}{day}.avro"


        # Save jobs avro
        s3.put_object(
            Bucket=os.environ['LINKEDIN_DATALAKE_KEY'],
            Key=f"raw/{jobs_base_key}/{file_name}",
            Body=avro_data
        )
        logger.info(f"Job data saved to S3: s3://{os.environ['LINKEDIN_DATALAKE_KEY']}/raw/{jobs_base_key}/")

        # Read existing job IDs from S3
        existing_jobs_response = s3.get_object(
            Bucket=os.environ['LINKEDIN_DATALAKE_KEY'],
            Key='raw/job_id.csv'
        )
        existing_jobs_content = existing_jobs_response['Body'].read().decode('utf-8')
        existing_jobs = [row.strip() for row in existing_jobs_content.split('\n') if row.strip()]

        # Append new job IDs to the list
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer)
        for existing_job in existing_jobs:
            csv_writer.writerow([existing_job])
        for job in all_jobs:
            csv_writer.writerow([job['job_id']])

        # Write back to S3
        s3.put_object(
            Bucket=os.environ['LINKEDIN_DATALAKE_KEY'],
            Key='raw/job_id.csv',
            Body=csv_buffer.getvalue()
        )
        logger.info("New job IDs appended to job_id.csv")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully processed jobs',
                'jobs_processed': len(all_jobs),
                'datalake_path': f"s3://{os.environ['LINKEDIN_DATALAKE_KEY']}/raw/{jobs_base_key}/"
            })
        }
        
    except Exception as e:
        logger.error(f"Error in lambda function: {str(e)}")
        raise

def lambda_handler(event, context):
    """Synchronous Lambda function handler"""
    return asyncio.run(async_lambda_handler(event, context))

if __name__ == "__main__":
    lambda_handler(None, None)


