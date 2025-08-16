from open_linkedin_api import Linkedin
from requests.cookies import RequestsCookieJar
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

#lk=Linkedin(username='datajr16001@gmail.com',password='Datajr@16')
lk=Linkedin(username='datajr16001@gmail.com',cookies='C:\Users\George\Desktop\JR AI engineer\JRDE16-JLA\Data Processing\datajr16001@gmail.com.jr')
def get_jobid(js):
    
      return js['trackingUrn'].split(':')[-1]
      
def get_jobdescription(js):
      return js['description']['text']
#demo

ak=lk.search_jobs(keywords="data engineer",offset=20)
job_item=ak[0]
jobid=get_jobid(job_item)
print(jobid)
job_info=lk.get_job(job_id=jobid)
title=job_info['title']
print(title)
company=job_info['companyDetails']['com.linkedin.voyager.deco.jobs.web.shared.WebJobPostingCompany']['companyResolutionResult']['name']
print(company)
location=job_info['companyDetails']['com.linkedin.voyager.deco.jobs.web.shared.WebJobPostingCompany']['companyResolutionResult']['headquarter']['city']
print(location)
employment_type=job_info['formattedEmploymentStatus']
print(employment_type)
seniority_level=job_info['formattedExperienceLevel']
print(seniority_level)
industry=job_info['formattedIndustries']
print(industry)
work_place=job_info['workplaceTypes'][0].split(':')[-1]
print(work_place)
job_description=job_info['description']['text']
print(job_description)
job_url=job_info['jobPostingUrl']
print(job_url)
repost=job_item['repostedJob']
print(repost)
post_date=job_info['listedAt']
print(post_date)
expire_date=job_info['expireAt']
print(expire_date)