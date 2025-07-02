from open_linkedin_api import Linkedin
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
lk=Linkedin(username='datajr16001@gmail.com',password='Datajr@16')
def get_jobid(js):
    
      return js['trackingUrn'].split(':')[-1]
      
def get_jobdescription(js):
      return js['description']['text']
#demo
ak=lk.search_jobs(keywords="data engineer")
jobid=get_jobid(ak[0])
print(jobid)
jobfull=lk.get_job(job_id=jobid)
print(jobfull)
jd=get_jobdescription(jobfull)
print(jd)