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
Linkedin.get_job=get_j
lk=Linkedin(username='datajr16001@gmail.com',password='Datajr@16')
def get_jobid(js):
      job_id = js[1]['trackingUrn'].split(':')[-1]
      return jobid
      
ak=lk.search_jobs(keywords="data engineer")
for i in ak:
      print(get_jobid(i))