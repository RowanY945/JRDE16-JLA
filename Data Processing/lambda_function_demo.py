from open_linkedin_api import Linkedin
import time
from time import sleep
import random
from requests.cookies import RequestsCookieJar
import boto3

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


# Create a RequestsCookieJar object to hold cookies
cookies = RequestsCookieJar()

# Set the cookies required for authentication
cookies.set('li_at', 'AQEDAVyKu8wB_TcWAAABl-3POjYAAAGYEdu-NlYAe1UalbTlcVY-Nh4hTgEe6gg0dvwX6uorJhBd-gFuXjJXCn8Vk8_VC0hA-qOi2Du4m5gy1NL3yhTJ-vQ1ognOZns1rCF5V31eNxTJa6oh3m8uPKvy', domain='www.linkedin.com')
cookies.set('JSESSIONID', 'ajax:4989259530814177005', domain='www.linkedin.com')

# Authenticate using the cookies
# Make sure to pass an empty password when using cookies
lk = Linkedin(cookies=cookies, username='yg515401@gmail.com', password='')



def get_jobid(js):
    
      return js['trackingUrn'].split(':')[-1]
      
def get_jobdescription(js):
      return js['description']['text']
#demo

def lambda_handler(event, context):
    ak=lk.search_jobs(keywords="data engineer",offset=0,limit=20)
    lenth=len(ak)
    print(len(ak))
    return {
        'statusCode': 200,
        'body': lenth
    }

