import os
import json
from typing import List, Dict, Any, Optional
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage
from qdrant_client import QdrantClient
from qdrant_client.models import QueryRequest
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()

# Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "job_postings")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")


class JobRetriever:
    def __init__(self):
        """Initialize the job retrieval system."""
        # Initialize embeddings
        self.embeddings = OpenAIEmbeddings(
            openai_api_key=OPENAI_API_KEY,
            model=EMBEDDING_MODEL
        )
        
        # Initialize Qdrant client
        if QDRANT_API_KEY:
            self.qdrant_client = QdrantClient(
                url=QDRANT_URL,
                api_key=QDRANT_API_KEY
            )
        else:
            self.qdrant_client = QdrantClient(url=QDRANT_URL)
        
        print(f"Connected to Qdrant at {QDRANT_URL}")
        print(f"Using collection: {COLLECTION_NAME}")
    
    def retrieve_matching_jobs(self, 
                              resume_text: str, 
                              k: int = 5,
                              score_threshold: Optional[float] = None) -> List[Dict[str, Any]]:
        """
        Retrieve the k most similar jobs based on resume text.
        
        Args:
            resume_text: The user's resume as text
            k: Number of top matches to return
            score_threshold: Optional minimum similarity score (0-1)
            
        Returns:
            List of matching jobs with their details and scores
        """
        print(f"\nSearching for top {k} matching jobs...")
        
        # Generate embedding for the resume
        resume_embedding = self.embeddings.embed_query(resume_text)
        
        search_results = self.qdrant_client.search(
            collection_name=COLLECTION_NAME,
            query_vector=resume_embedding,
            limit=k,
            score_threshold=score_threshold
        )
        
        # Format results
        matches = []
        for idx, result in enumerate(search_results, 1):
            match_data = {
                'rank': idx,
                'job_posting_id': result.payload.get('job_posting_id'),
                'similarity_score': result.score,
                'skills_required': result.payload.get('skills', []),
                'job_description': result.payload.get('text', '')[:500] + '...'  # First 500 chars
            }
            matches.append(match_data)
        
        return matches
    
    def analyze_skill_gaps(self, 
                          user_skills: List[str], 
                          matched_jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Analyze skill gaps between user's skills and job requirements.
        
        Args:
            user_skills: List of skills from user's resume
            matched_jobs: List of matched jobs from retrieve_matching_jobs
            
        Returns:
            List of jobs with skill gap analysis
        """
        user_skills_lower = [skill.lower().strip() for skill in user_skills]
        
        analyzed_jobs = []
        for job in matched_jobs:
            required_skills = job.get('skills_required', [])
            required_skills_lower = [skill.lower().strip() for skill in required_skills]
            
            # Calculate skill matches and gaps
            matching_skills = [
                skill for skill in required_skills 
                if skill.lower().strip() in user_skills_lower
            ]
            
            missing_skills = [
                skill for skill in required_skills 
                if skill.lower().strip() not in user_skills_lower
            ]
            
            skill_match_percentage = (
                len(matching_skills) / len(required_skills) * 100 
                if required_skills else 0
            )
            
            analyzed_job = {
                **job,  # Include all original job data
                'matching_skills': matching_skills,
                'missing_skills': missing_skills,
                'skill_match_percentage': round(skill_match_percentage, 1),
                'total_required_skills': len(required_skills)
            }
            
            analyzed_jobs.append(analyzed_job)
        
        # Sort by skill match percentage (secondary sort after similarity score)
        analyzed_jobs.sort(
            key=lambda x: (x['similarity_score'], x['skill_match_percentage']), 
            reverse=True
        )
        
        return analyzed_jobs
    
    def extract_skills_from_resume(self, resume_text: str) -> List[str]:
        """
        Use OpenAI LLM to extract technical/hard skills from resume text.
        Excludes soft skills like leadership, communication, etc.
        
        Args:
            resume_text: The user's resume as text
            
        Returns:
            List of extracted technical skills
        """
        # Initialize the LLM (already imported at the top)
        llm = ChatOpenAI(
            temperature=0.3,
            model="gpt-4o-mini",  # or gpt-4 for better accuracy
            openai_api_key=OPENAI_API_KEY
        )
        
        # Create the prompt
        system_prompt = """You are a technical recruiter expert at identifying technical/hard skills from resumes.
        
Your task is to extract ONLY technical/hard skills from the resume text. 

EXCLUDE:
- Soft skills (leadership, communication, teamwork, problem-solving, etc.)
- Generic business skills (project management, strategic planning, etc.)
- Personal qualities (motivated, dedicated, passionate, etc.)
- Non-technical certifications
- Years of experience
- Job titles
- Company names
- Educational degrees (unless they are technical certifications like AWS Certified, etc.)

Return ONLY a JSON array of technical skills found in the resume. 
Example output: ["Python", "React", "AWS", "Docker", "PostgreSQL"]

If no technical skills are found, return an empty array: []"""

        human_prompt = f"Extract technical skills from this resume:\n\n{resume_text}"
        
        try:
            # Get response from LLM
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=human_prompt)
            ]
            
            response = llm.invoke(messages)
            
            # Parse the JSON response
            import json
            skills = json.loads(response.content)
            
            # Ensure it's a list
            if isinstance(skills, list):
                # Clean and deduplicate
                skills = list(set([skill.strip() for skill in skills if skill.strip()]))
                return sorted(skills)  # Return sorted for consistency
            else:
                print("Warning: LLM did not return a list. Using empty skill list.")
                return []
                
        except json.JSONDecodeError:
            print("Warning: Could not parse LLM response as JSON. Attempting to extract from response.")
            # Try to extract JSON array from the response
            response_text = response.content
            if '[' in response_text and ']' in response_text:
                try:
                    start = response_text.index('[')
                    end = response_text.rindex(']') + 1
                    skills = json.loads(response_text[start:end])
                    if isinstance(skills, list):
                        return sorted(list(set([skill.strip() for skill in skills if skill.strip()])))
                except:
                    pass
            
            print("Could not extract skills from LLM response. Returning empty list.")
            return []
            
        except Exception as e:
            print(f"Error extracting skills with LLM: {e}")
            print("Please check your OpenAI API key and connection.")
            return []
    
    def display_results(self, analyzed_jobs: List[Dict[str, Any]]):
        """
        Display the retrieval and analysis results in a formatted way.
        
        Args:
            analyzed_jobs: List of jobs with analysis from analyze_skill_gaps
        """
        print("\n" + "="*80)
        print("JOB MATCHING RESULTS")
        print("="*80)
        
        for job in analyzed_jobs:
            print(f"\n--- Rank #{job['rank']} ---")
            print(f"Job ID: {job['job_posting_id']}")
            print(f"Similarity Score: {job['similarity_score']:.3f}")
            print(f"Skill Match: {job['skill_match_percentage']}% ({len(job['matching_skills'])}/{job['total_required_skills']} skills)")
            
            if job['matching_skills']:
                print(f"✅ Your Matching Skills: {', '.join(job['matching_skills'])}")
            
            if job['missing_skills']:
                print(f"⚠️  Skills to Learn: {', '.join(job['missing_skills'])}")
            
            print(f"\nJob Preview:")
            print(job['job_description'])
            print("-" * 80)
    
    def save_results_to_csv(self, analyzed_jobs: List[Dict[str, Any]], filename: str = "job_matches.csv"):
        """
        Save the results to a CSV file for further analysis.
        
        Args:
            analyzed_jobs: List of jobs with analysis
            filename: Output CSV filename
        """
        # Prepare data for DataFrame
        rows = []
        for job in analyzed_jobs:
            rows.append({
                'rank': job['rank'],
                'job_posting_id': job['job_posting_id'],
                'similarity_score': job['similarity_score'],
                'skill_match_percentage': job['skill_match_percentage'],
                'matching_skills': ', '.join(job['matching_skills']),
                'missing_skills': ', '.join(job['missing_skills']),
                'total_required_skills': job['total_required_skills']
            })
        
        df = pd.DataFrame(rows)
        df.to_csv(filename, index=False)
        print(f"\nResults saved to {filename}")


def main():
    """Main function to demonstrate the retrieval system."""
    
    # Example resume text (replace with actual resume)
    sample_resume = """
Alex Chen
San Francisco, CA | (415) 555-0123 | alex.chen@email.com | linkedin.com/in/alexchen-data | github.com/alexchen-data

Senior Data Engineer | Cloud & Real-Time Streaming Specialist
Data engineer with 9+ years of experience architecting and building robust, scalable data platforms in both fast-paced enterprise and start-up environments. Expert in implementing modern data architectures like Data Mesh and Lakehouse using Databricks, Spark, and Kafka to solve complex business problems. Proven success in client-facing consulting roles, translating technical requirements into strategic data solutions that drive efficiency and growth.

Core Competencies
Data Architecture: Data Mesh, Lakehouse, ETL/ELT, Data Modeling, Real-time Streaming

Big Data Technologies: Databricks, Apache Spark, SQL, Snowflake, Kafka, Flink

Cloud Platforms: AWS (S3, Redshift, IAM, Glue), GCP (BigQuery, Dataflow, Pub/Sub)

NoSQL Databases: Cassandra, MongoDB

DevOps & IaC: CI/CD (GitLab CI), Terraform, Docker, Kubernetes

Compliance: Data Security, GDPR, HIPAA, Data Governance

Professional Experience
Lead Data Engineer | Vertex Consulting, San Francisco, CA | 2019 – Present
(Global technology consultancy and professional services firm)

Led a cross-functional team to design and implement a client's first Data Mesh architecture, decentralizing data ownership across 5 business domains and reducing time-to-insight by 50%.

Architected a cost-effective, multi-cloud data solution utilizing AWS S3 for storage, Databricks for processing, and Google BigQuery as the enterprise data warehouse, serving over 1,000 daily active users.

Developed and maintained mission-critical, low-latency data pipelines using Apache Kafka and Flink to process and enrich real-time event streams for a client's customer analytics platform.

Acted as the primary technical advisor for clients, conducting workshops to gather requirements, present architectural designs, and ensure solutions aligned with business goals and security policies.

Implemented robust data security frameworks and access controls across AWS Redshift and Snowflake environments, ensuring compliance with industry regulations for healthcare and financial data.

Data Engineer | Nimbus Analytics (Acquired by TechGlobal), Austin, TX | 2015 – 2019
*(Series-B B2B SaaS start-up)*

Built the foundational data infrastructure from the ground up, designing scalable ETL pipelines with Apache Spark to ingest and process data from diverse sources into a centralized data lake on AWS S3.

Optimized and tuned performance of MongoDB and Cassandra clusters to support the company's high-throughput, document-based primary application, improving read query performance by 35%.

Instrumented and deployed logging and monitoring for all data pipelines, increasing system reliability and reducing mean time to resolution (MTTR) for data issues.

Key Projects
Unified Logistics Dashboard | Vertex Consulting

Challenge: A logistics client needed a single view of nationwide operations from disparate, siloed systems.

Solution: Engineered a real-time pipeline using Kafka to stream IoT sensor data and Databricks Spark structured streaming jobs to aggregate and join it with transactional data from Snowflake.

Result: Enabled live tracking and predictive ETAs, decreasing fuel costs by 15% through optimized routing.

Education
Bachelor of Science, Computer Science | Stanford University, Stanford, CA

Certifications & Continuous Learning
AWS Certified Data Analytics – Specialty

Confluent Certified Developer for Apache Kafka (CCDAK)

Databricks Lakehouse Fundamentals

Ongoing coursework on Data Mesh principles and advanced Flink applications.
    """
    
    # Initialize retriever
    retriever = JobRetriever()
    
    # Option 1: Use the sample resume above
    resume_text = sample_resume
    
    # Option 2: Load resume from file
    # with open('resume.txt', 'r') as f:
    #     resume_text = f.read()
    
    # Retrieve matching jobs
    k = 10  # Get top 10 matches
    matched_jobs = retriever.retrieve_matching_jobs(
        resume_text=resume_text,
        k=k,
        score_threshold=0.5  # Optional: only return jobs with >0.5 similarity
    )
    
    if not matched_jobs:
        print("No matching jobs found!")
        return
    
    # Extract skills from resume using AI
    print("\nExtracting technical skills from resume using AI...")
    user_skills = retriever.extract_skills_from_resume(resume_text)
    print(f"Extracted {len(user_skills)} technical skills: {', '.join(sorted(user_skills))}")
    
    # Analyze skill gaps
    analyzed_jobs = retriever.analyze_skill_gaps(user_skills, matched_jobs)
    
    # Display results
    retriever.display_results(analyzed_jobs)
    
    # Save to CSV
    retriever.save_results_to_csv(analyzed_jobs)
    
    # Summary statistics
    print("\n" + "="*80)
    print("SUMMARY STATISTICS")
    print("="*80)
    avg_similarity = sum(job['similarity_score'] for job in analyzed_jobs) / len(analyzed_jobs)
    avg_skill_match = sum(job['skill_match_percentage'] for job in analyzed_jobs) / len(analyzed_jobs)
    
    print(f"Average Similarity Score: {avg_similarity:.3f}")
    print(f"Average Skill Match: {avg_skill_match:.1f}%")
    
    # Find most common missing skills
    all_missing_skills = []
    for job in analyzed_jobs:
        all_missing_skills.extend(job['missing_skills'])
    
    if all_missing_skills:
        from collections import Counter
        skill_counter = Counter(all_missing_skills)
        print("\nTop Skills to Learn (across all matches):")
        for skill, count in skill_counter.most_common(5):
            print(f"  - {skill}: needed for {count} jobs")


if __name__ == "__main__":
    main()