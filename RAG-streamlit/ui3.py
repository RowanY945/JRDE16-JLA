import streamlit as st
import os
import json
from typing import List, Dict, Any, Optional
from langchain_openai import OpenAIEmbeddings, ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage
from qdrant_client import QdrantClient
from dotenv import load_dotenv
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import PyPDF2
import docx
import io
from pydantic import BaseModel, Field
from langchain_core.prompts import ChatPromptTemplate

# Load environment variables
load_dotenv()

# Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "job_postings")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")

# Page configuration
st.set_page_config(
    page_title="AI Job Matcher",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stAlert {
        margin-top: 1rem;
    }
    .job-card {
        background-color: #f0f2f6;
        padding: 1.5rem;
        border-radius: 10px;
        margin-bottom: 1rem;
        border-left: 4px solid #1f77b4;
    }
    .skill-badge {
        background-color: #e1f5fe;
        color: #01579b;
        padding: 0.25rem 0.75rem;
        border-radius: 15px;
        margin: 0.25rem;
        display: inline-block;
        font-size: 0.875rem;
    }
    .missing-skill-badge {
        background-color: #fff3e0;
        color: #e65100;
        padding: 0.25rem 0.75rem;
        border-radius: 15px;
        margin: 0.25rem;
        display: inline-block;
        font-size: 0.875rem;
    }
    .chat-message {
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 1rem;
        display: flex;
    }
    .chat-message.user {
        background-color: #e1f5fe;
    }
    .chat-message.assistant {
        background-color: #f0f2f6;
    }
    </style>
    """, unsafe_allow_html=True)


# Define Pydantic model for structured output
class ResumeSkills(BaseModel):
    technical_skills: List[str] = Field(description="List of technical skills extracted from the resume")


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
    
    def retrieve_matching_jobs(self, 
                              resume_text: str, 
                              k: int = 5,
                              score_threshold: Optional[float] = None) -> List[Dict[str, Any]]:
        """Retrieve the k most similar jobs based on resume text."""
        # Generate embedding for the resume
        resume_embedding = self.embeddings.embed_query(resume_text)
        
        # Search in Qdrant
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
                'job_description': result.payload.get('text', '')
            }
            matches.append(match_data)
        
        return matches
    
    def extract_skills_from_resume(self, resume_text: str) -> List[str]:
        """Use OpenAI LLM to extract technical/hard skills from resume text."""
        llm = ChatOpenAI(
            temperature=0.3,
            model="gpt-4o-mini",
            openai_api_key=OPENAI_API_KEY
        )
        
        # Create structured output LLM
        structured_llm = llm.with_structured_output(ResumeSkills)
        
        # Create prompt template
        prompt = ChatPromptTemplate.from_messages([
            ("system", """You are a technical recruiter expert at identifying technical/hard skills from resumes.
            
Extract ONLY technical/hard skills from the resume text. 

EXCLUDE soft skills, personal qualities, job titles, company names, and educational degrees.

Return ONLY a list of technical skills found in the resume."""),
            ("human", "Extract technical skills from this resume:\n\n{resume_text}")
        ])
        
        try:
            # Create chain
            chain = prompt | structured_llm
            
            # Invoke chain
            result = chain.invoke({"resume_text": resume_text})
            
            if result and hasattr(result, 'technical_skills'):
                skills = list(set([skill.strip() for skill in result.technical_skills if skill.strip()]))
                return sorted(skills)
            return []
                
        except Exception as e:
            st.error(f"Error extracting skills: {e}")
            return []
    
    def analyze_skill_gaps(self, 
                          user_skills: List[str], 
                          matched_jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Analyze skill gaps between user's skills and job requirements."""
        user_skills_lower = [skill.lower().strip() for skill in user_skills]
        
        analyzed_jobs = []
        for job in matched_jobs:
            required_skills = job.get('skills_required', [])
            
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
                **job,
                'matching_skills': matching_skills,
                'missing_skills': missing_skills,
                'skill_match_percentage': round(skill_match_percentage, 1),
                'total_required_skills': len(required_skills)
            }
            
            analyzed_jobs.append(analyzed_job)
        
        analyzed_jobs.sort(
            key=lambda x: (x['similarity_score'], x['skill_match_percentage']), 
            reverse=True
        )
        
        return analyzed_jobs


def extract_text_from_pdf(pdf_file):
    """Extract text from PDF file."""
    pdf_reader = PyPDF2.PdfReader(pdf_file)
    text = ""
    for page in pdf_reader.pages:
        text += page.extract_text()
    return text


def extract_text_from_docx(docx_file):
    """Extract text from DOCX file."""
    doc = docx.Document(docx_file)
    text = ""
    for paragraph in doc.paragraphs:
        text += paragraph.text + "\n"
    return text


def extract_text_from_file(uploaded_file):
    """Extract text from uploaded file based on file type."""
    if uploaded_file.type == "application/pdf":
        return extract_text_from_pdf(uploaded_file)
    elif uploaded_file.type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
        return extract_text_from_docx(uploaded_file)
    elif uploaded_file.type == "text/plain":
        return str(uploaded_file.read(), "utf-8")
    else:
        return None


def create_skills_gap_analysis(all_missing_skills):
    """Create a pie chart of most needed skills."""
    from collections import Counter
    
    if not all_missing_skills:
        return None
    
    skill_counter = Counter(all_missing_skills)
    top_skills = dict(skill_counter.most_common(10))
    
    fig = px.pie(
        values=list(top_skills.values()),
        names=list(top_skills.keys()),
        title="Top Skills to Learn (Across All Matches)",
        hole=0.4
    )
    
    fig.update_traces(textposition='inside', textinfo='percent+label')
    fig.update_layout(height=400)
    
    return fig


def initialize_session_state():
    """Initialize all session state variables."""
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
    
    if "resume_text" not in st.session_state:
        st.session_state.resume_text = ""
    
    if "analyzed_jobs" not in st.session_state:
        st.session_state.analyzed_jobs = None
    
    if "user_skills" not in st.session_state:
        st.session_state.user_skills = []
    
    if "processed_resume" not in st.session_state:
        st.session_state.processed_resume = False


def add_message_to_chat(role, content):
    """Add a message to the chat history."""
    st.session_state.chat_history.append({"role": role, "content": content})


def display_chat_message(role, content):
    """Display a chat message."""
    with st.chat_message(role):
        st.markdown(content)


def main():
    st.title("🚀 AI-Powered Job Matcher")
    st.markdown("Upload your resume and get personalized job recommendations with skill gap analysis")
    
    # Initialize session state
    initialize_session_state()
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("⚙️ Settings")
        
        num_jobs = st.slider(
            "Number of job recommendations",
            min_value=5,
            max_value=20,
            value=10,
            step=1
        )
        
        score_threshold = st.slider(
            "Minimum similarity score",
            min_value=0.0,
            max_value=1.0,
            value=0.3,
            step=0.05,
            help="Lower values return more matches"
        )
        
        # Clear results button
        if st.button("🗑️ Clear All Results", use_container_width=True):
            st.session_state.analyzed_jobs = None
            st.session_state.user_skills = []
            st.session_state.processed_resume = False
            st.session_state.chat_history = []
            st.rerun()
        
        st.divider()
        
        st.header("📊 Statistics")
        if st.session_state.analyzed_jobs:
            jobs = st.session_state.analyzed_jobs
            avg_similarity = sum(job['similarity_score'] for job in jobs) / len(jobs)
            avg_skill_match = sum(job['skill_match_percentage'] for job in jobs) / len(jobs)
            
            st.metric("Avg Similarity Score", f"{avg_similarity:.2%}")
            st.metric("Avg Skill Match", f"{avg_skill_match:.1f}%")
            st.metric("Jobs Found", len(jobs))
    
    # Main content area - tabs for different functionalities
    tab1, tab2 = st.tabs(["🔍 Job Matching", "💬 Resume Chat"])
    
    with tab1:
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.header("📄 Upload Resume")
            
            # File uploader
            uploaded_file = st.file_uploader(
                "Choose a file",
                type=['pdf', 'docx', 'txt'],
                help="Supported formats: PDF, DOCX, TXT"
            )
            
            # Text area for manual input
            st.markdown("**Or paste your resume text:**")
            resume_text = st.text_area(
                "Resume text",
                height=300,
                placeholder="Paste your resume here...",
                label_visibility="collapsed",
                key="resume_text_input"
            )
            
            # Process button
            process_button = st.button(
                "🔍 Find Matching Jobs",
                type="primary",
                use_container_width=True,
                disabled=not (uploaded_file or resume_text)
            )
        
        with col2:
            # Show existing results if available
            if st.session_state.analyzed_jobs:
                analyzed_jobs = st.session_state.analyzed_jobs
                user_skills = st.session_state.user_skills
                
                # Display extracted skills
                st.success(f"✅ Found {len(user_skills)} technical skills in your resume")
                
                if user_skills:
                    st.markdown("**Your Technical Skills:**")
                    skills_html = "".join([f'<span class="skill-badge">{skill}</span>' for skill in user_skills])
                    st.markdown(skills_html, unsafe_allow_html=True)
                
                st.divider()
                
                # Display visualization
                st.header("📈 Skills Gap Analysis")
                
                all_missing_skills = []
                for job in analyzed_jobs:
                    all_missing_skills.extend(job['missing_skills'])
                
                fig = create_skills_gap_analysis(all_missing_skills)
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
                
                st.divider()
                
                # Display job recommendations
                st.header(f"💼 Top {len(analyzed_jobs)} Job Recommendations")
                
                # Tabs for different views
                view_tab1, view_tab2, view_tab3 = st.tabs(["📋 Detailed View", "📊 Table View", "💾 Export"])
                
                with view_tab1:
                    for job in analyzed_jobs:
                        with st.expander(
                            f"**Rank #{job['rank']}** - Job ID: {job['job_posting_id']} "
                            f"(Match: {job['similarity_score']:.1%} | Skills: {job['skill_match_percentage']:.1f}%)",
                            expanded=(job['rank'] <= 3)
                        ):
                            col_job1, col_job2 = st.columns([2, 1])
                            
                            with col_job1:
                                # Job description preview
                                st.markdown("**Job Description:**")
                                st.text(job['job_description'][:500] + "..." if len(job['job_description']) > 500 else job['job_description'])
                            
                            with col_job2:
                                # Metrics
                                st.metric("Similarity Score", f"{job['similarity_score']:.1%}")
                                st.metric("Skill Match", f"{job['skill_match_percentage']:.1f}%")
                                st.metric("Skills Matched", f"{len(job['matching_skills'])}/{job['total_required_skills']}")
                            
                            # Skills analysis
                            if job['matching_skills']:
                                st.markdown("**✅ Your Matching Skills:**")
                                skills_html = "".join([f'<span class="skill-badge">{skill}</span>' for skill in job['matching_skills']])
                                st.markdown(skills_html, unsafe_allow_html=True)
                            
                            if job['missing_skills']:
                                st.markdown("**📚 Skills to Learn:**")
                                skills_html = "".join([f'<span class="missing-skill-badge">{skill}</span>' for skill in job['missing_skills']])
                                st.markdown(skills_html, unsafe_allow_html=True)
                
                with view_tab2:
                    # Create DataFrame for table view
                    df = pd.DataFrame([
                        {
                            'Rank': job['rank'],
                            'Job ID': job['job_posting_id'],
                            'Similarity': f"{job['similarity_score']:.1%}",
                            'Skill Match': f"{job['skill_match_percentage']:.1f}%",
                            'Matching Skills': ', '.join(job['matching_skills']) if job['matching_skills'] else 'None',
                            'Missing Skills': ', '.join(job['missing_skills']) if job['missing_skills'] else 'None'
                        }
                        for job in analyzed_jobs
                    ])
                    
                    st.dataframe(df, use_container_width=True, hide_index=True)
                
                with view_tab3:
                    # Export options
                    st.markdown("**Export Results:**")
                    
                    # CSV export
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download as CSV",
                        data=csv,
                        file_name=f"job_matches_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                    
                    # JSON export
                    json_data = json.dumps(analyzed_jobs, indent=2)
                    st.download_button(
                        label="📥 Download as JSON",
                        data=json_data,
                        file_name=f"job_matches_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json"
                    )
            
            # Process new resume if button clicked
            if process_button:
                # Extract text from uploaded file or use pasted text
                if uploaded_file:
                    with st.spinner("Reading file..."):
                        extracted_text = extract_text_from_file(uploaded_file)
                        if extracted_text:
                            resume_text = extracted_text
                        else:
                            st.error("Could not extract text from file. Please try a different format.")
                            return
                
                if not resume_text:
                    st.error("Please upload a resume or paste text")
                    return
                
                # Store resume text in session state
                st.session_state.resume_text = resume_text
                
                # Initialize retriever
                with st.spinner("Initializing AI system..."):
                    try:
                        retriever = JobRetriever()
                    except Exception as e:
                        st.error(f"Error connecting to services: {e}")
                        return
                
                # Extract skills
                with st.spinner("Extracting skills from resume..."):
                    user_skills = retriever.extract_skills_from_resume(resume_text)
                    st.session_state.user_skills = user_skills
                    
                # Retrieve matching jobs
                with st.spinner(f"Finding top {num_jobs} matching jobs..."):
                    matched_jobs = retriever.retrieve_matching_jobs(
                        resume_text=resume_text,
                        k=num_jobs,
                        score_threshold=score_threshold
                    )
                
                if not matched_jobs:
                    st.warning("No matching jobs found. Try lowering the similarity threshold.")
                    return
                
                # Analyze skill gaps
                analyzed_jobs = retriever.analyze_skill_gaps(user_skills, matched_jobs)
                st.session_state.analyzed_jobs = analyzed_jobs
                st.session_state.processed_resume = True
                
                # Rerun to show results
                st.rerun()
    
    with tab2:
        st.header("💬 Chat About Your Resume")
        
        # Initialize LLM for chat
        if "chat_llm" not in st.session_state:
            st.session_state.chat_llm = ChatOpenAI(
                temperature=0.7,
                model="gpt-4o-mini",
                openai_api_key=OPENAI_API_KEY
            )
        
        # Display chat history
        for message in st.session_state.chat_history:
            display_chat_message(message["role"], message["content"])
        
        # Chat input
        if prompt := st.chat_input("Ask about your resume or career options..."):
            # Add user message to chat history
            add_message_to_chat("user", prompt)
            display_chat_message("user", prompt)
            
            # Prepare context for the LLM
            context = ""
            if st.session_state.resume_text:
                context = f"Here is the user's resume content for context:\n\n{st.session_state.resume_text[:4000]}"
            
            # Create system message with context
            system_message = f"""You are a helpful career advisor. Assist the user with questions about their resume, 
            career options, skill development, or job search strategies. 

            {context}
            
            Be specific, helpful, and encouraging in your responses."""
            
            # Generate response
            with st.spinner("Thinking..."):
                try:
                    # Prepare messages for the LLM
                    messages = [
                        SystemMessage(content=system_message),
                    ]
                    
                    # Add chat history (last 6 messages to avoid context overflow)
                    for msg in st.session_state.chat_history[-6:]:
                        if msg["role"] == "user":
                            messages.append(HumanMessage(content=msg["content"]))
                        else:
                            messages.append(SystemMessage(content=msg["content"]))
                    
                    response = st.session_state.chat_llm.invoke(messages)
                    
                    # Add assistant response to chat history
                    add_message_to_chat("assistant", response.content)
                    display_chat_message("assistant", response.content)
                    
                except Exception as e:
                    error_msg = f"Sorry, I encountered an error: {str(e)}"
                    add_message_to_chat("assistant", error_msg)
                    display_chat_message("assistant", error_msg)
        
        # Add some suggested questions
        st.markdown("---")
        st.markdown("**💡 Try asking:**")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("How can I improve my resume?", key="btn1"):
                if "resume_text" in st.session_state and st.session_state.resume_text:
                    add_message_to_chat("user", "How can I improve my resume?")
                    display_chat_message("user", "How can I improve my resume?")
                    
                    # Prepare context
                    context = f"Here is the user's resume content for context:\n\n{st.session_state.resume_text[:4000]}"
                    system_message = f"""You are a helpful career advisor. Provide specific suggestions to improve this resume.
                    
                    {context}
                    
                    Focus on format, content, keywords, and overall presentation."""
                    
                    with st.spinner("Thinking..."):
                        try:
                            messages = [
                                SystemMessage(content=system_message),
                                HumanMessage(content="How can I improve my resume?")
                            ]
                            
                            response = st.session_state.chat_llm.invoke(messages)
                            add_message_to_chat("assistant", response.content)
                            display_chat_message("assistant", response.content)
                        except Exception as e:
                            error_msg = f"Sorry, I encountered an error: {str(e)}"
                            add_message_to_chat("assistant", error_msg)
                            display_chat_message("assistant", error_msg)
                else:
                    st.warning("Please process a resume first in the Job Matching tab.")
            
            if st.button("What skills should I learn next?", key="btn3"):
                if st.session_state.user_skills and st.session_state.analyzed_jobs:
                    add_message_to_chat("user", "What skills should I learn next?")
                    display_chat_message("user", "What skills should I learn next?")
                    
                    # Prepare context
                    user_skills = ", ".join(st.session_state.user_skills)
                    missing_skills = []
                    for job in st.session_state.analyzed_jobs:
                        missing_skills.extend(job['missing_skills'])
                    
                    from collections import Counter
                    top_missing = Counter(missing_skills).most_common(5)
                    top_missing_str = ", ".join([skill for skill, count in top_missing])
                    
                    context = f"""The user has these skills: {user_skills}
                    
                    Based on job matching analysis, these are the most frequently missing skills: {top_missing_str}"""
                    
                    system_message = f"""You are a helpful career advisor. Suggest specific skills the user should learn next based on their current skills and job market needs.
                    
                    {context}
                    
                    Provide specific, actionable advice."""
                    
                    with st.spinner("Thinking..."):
                        try:
                            messages = [
                                SystemMessage(content=system_message),
                                HumanMessage(content="What skills should I learn next?")
                            ]
                            
                            response = st.session_state.chat_llm.invoke(messages)
                            add_message_to_chat("assistant", response.content)
                            display_chat_message("assistant", response.content)
                        except Exception as e:
                            error_msg = f"Sorry, I encountered an error: {str(e)}"
                            add_message_to_chat("assistant", error_msg)
                            display_chat_message("assistant", error_msg)
                else:
                    st.warning("Please process a resume first in the Job Matching tab.")
        
        with col2:
            if st.button("What jobs fit my skills best?", key="btn2"):
                if st.session_state.user_skills:
                    add_message_to_chat("user", "What jobs fit my skills best?")
                    display_chat_message("user", "What jobs fit my skills best?")
                    
                    # Prepare context
                    user_skills = ", ".join(st.session_state.user_skills)
                    
                    context = f"The user has these skills: {user_skills}"
                    
                    system_message = f"""You are a helpful career advisor. Suggest job roles and industries that would be a good fit for the user's skills.
                    
                    {context}
                    
                    Be specific and provide examples of job titles."""
                    
                    with st.spinner("Thinking..."):
                        try:
                            messages = [
                                SystemMessage(content=system_message),
                                HumanMessage(content="What jobs fit my skills best?")
                            ]
                            
                            response = st.session_state.chat_llm.invoke(messages)
                            add_message_to_chat("assistant", response.content)
                            display_chat_message("assistant", response.content)
                        except Exception as e:
                            error_msg = f"Sorry, I encountered an error: {str(e)}"
                            add_message_to_chat("assistant", error_msg)
                            display_chat_message("assistant", error_msg)
                else:
                    st.warning("Please process a resume first in the Job Matching tab.")
            
            if st.button("Suggest career paths for me", key="btn4"):
                if st.session_state.user_skills:
                    add_message_to_chat("user", "Suggest career paths for me")
                    display_chat_message("user", "Suggest career paths for me")
                    
                    # Prepare context
                    user_skills = ", ".join(st.session_state.user_skills)
                    
                    context = f"The user has these skills: {user_skills}"
                    
                    system_message = f"""You are a helpful career advisor. Suggest potential career paths for the user based on their skills.
                    
                    {context}
                    
                    Provide 3-5 specific career paths with brief descriptions."""
                    
                    with st.spinner("Thinking..."):
                        try:
                            messages = [
                                SystemMessage(content=system_message),
                                HumanMessage(content="Suggest career paths for me")
                            ]
                            
                            response = st.session_state.chat_llm.invoke(messages)
                            add_message_to_chat("assistant", response.content)
                            display_chat_message("assistant", response.content)
                        except Exception as e:
                            error_msg = f"Sorry, I encountered an error: {str(e)}"
                            add_message_to_chat("assistant", error_msg)
                            display_chat_message("assistant", error_msg)
                else:
                    st.warning("Please process a resume first in the Job Matching tab.")
    
    # Footer
    st.divider()
    st.markdown(
        """
        <div style='text-align: center; color: gray; padding: 1rem;'>
        Built with ❤️ using Streamlit, LangChain, and Qdrant
        </div>
        """,
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()