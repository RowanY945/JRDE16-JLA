import pandas as pd
import json
from typing import List, Dict, Any
from langchain.embeddings import OpenAIEmbeddings
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.schema import Document
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
import uuid
from tqdm import tqdm
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")  # Full URL format
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "")  # Optional for local, required for cloud
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "job_postings")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")  # or text-embedding-ada-002
VECTOR_SIZE = int(os.getenv("VECTOR_SIZE", 1536))  # for text-embedding-3-small
PARQUET_FILE_PATH = os.getenv("PARQUET_FILE_PATH", "job_postings.parquet")

class JobEmbeddingPipeline:
    def __init__(self, 
                 parquet_path: str,
                 qdrant_url: str = QDRANT_URL,
                 qdrant_api_key: str = QDRANT_API_KEY,
                 collection_name: str = COLLECTION_NAME):
        """
        Initialize the job embedding pipeline.
        
        Args:
            parquet_path: Path to the parquet file containing job data
            qdrant_url: Qdrant server URL (e.g., "http://localhost:6333" or "https://xxx.qdrant.io")
            qdrant_api_key: Qdrant API key (optional for local, required for cloud)
            collection_name: Name of the Qdrant collection
        """
        self.parquet_path = parquet_path
        self.collection_name = collection_name
        
        # Initialize embeddings
        self.embeddings = OpenAIEmbeddings(
            openai_api_key=OPENAI_API_KEY,
            model=EMBEDDING_MODEL
        )
        
        # Initialize Qdrant client
        if qdrant_api_key:
            self.qdrant_client = QdrantClient(
                url=qdrant_url,
                api_key=qdrant_api_key
            )
        else:
            # Local deployment without API key
            self.qdrant_client = QdrantClient(url=qdrant_url)
        
        # Test connection
        print(f"Connected to Qdrant. Collections: {self.qdrant_client.get_collections()}")
        
        # Create collection if it doesn't exist
        self._create_collection_if_not_exists()
    
    def _create_collection_if_not_exists(self):
        """Create Qdrant collection if it doesn't exist."""
        collections = self.qdrant_client.get_collections().collections
        collection_names = [collection.name for collection in collections]
        
        if self.collection_name not in collection_names:
            self.qdrant_client.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(
                    size=VECTOR_SIZE,
                    distance=Distance.COSINE
                )
            )
            print(f"Created collection: {self.collection_name}")
        else:
            print(f"Collection {self.collection_name} already exists")
    
    def _format_job_for_embedding(self, job_posting_id: str, ai_summary: str) -> str:
        """
        Format job posting data into a text chunk for embedding.
        
        Args:
            job_posting_id: The job posting ID
            ai_summary: The AI-generated summary in JSON format
            
        Returns:
            Formatted text string for embedding
        """
        try:
            # Parse the JSON summary
            summary_dict = json.loads(ai_summary) if isinstance(ai_summary, str) else ai_summary
            
            # Build a comprehensive text representation
            text_parts = [
                f"Job ID: {job_posting_id}",
                f"Title: {summary_dict.get('job_title', 'N/A')}",
                f"Seniority: {summary_dict.get('job_seniority_level', 'N/A')}",
                f"Workplace: {summary_dict.get('workplace_type', 'N/A')}",
                ""
            ]
            
            # Add overview
            if overview := summary_dict.get('overview'):
                text_parts.append(f"Overview: {overview}")
                text_parts.append("")
            
            # Add responsibilities
            if responsibilities := summary_dict.get('responsibilities'):
                text_parts.append("Responsibilities:")
                for resp in responsibilities:
                    text_parts.append(f"- {resp}")
                text_parts.append("")
            
            # Add requirements
            if requirements := summary_dict.get('requirements'):
                # Skills
                if skills := requirements.get('skills'):
                    text_parts.append(f"Required Skills: {', '.join(skills)}")
                
                # Qualifications
                if qualifications := requirements.get('qualifications'):
                    text_parts.append("Qualifications:")
                    for qual in qualifications:
                        text_parts.append(f"- {qual}")
                
                # Experience
                if experience := requirements.get('experience'):
                    if years_min := experience.get('years_min'):
                        text_parts.append(f"Minimum Experience: {years_min} years")
                text_parts.append("")
            
            # Add domain classification
            if domain := summary_dict.get('domain_classification'):
                domain_text = []
                for category, subcategories in domain.items():
                    if isinstance(subcategories, list):
                        domain_text.append(f"{category}: {', '.join(subcategories)}")
                    else:
                        domain_text.append(f"{category}: {subcategories}")
                if domain_text:
                    text_parts.append(f"Domain: {'; '.join(domain_text)}")
            
            # Add compensation if available
            if compensation := summary_dict.get('compensation'):
                if salary := compensation.get('salary'):
                    salary_parts = []
                    if salary.get('min'):
                        salary_parts.append(f"Min: ${salary['min']}")
                    if salary.get('max'):
                        salary_parts.append(f"Max: ${salary['max']}")
                    if salary_parts:
                        text_parts.append(f"Salary: {', '.join(salary_parts)}")
            
            return "\n".join(text_parts)
            
        except (json.JSONDecodeError, TypeError) as e:
            print(f"Error parsing job {job_posting_id}: {e}")
            # Fallback to simple concatenation
            return f"Job ID: {job_posting_id}\n{ai_summary}"
    
    def load_and_process_data(self) -> List[Document]:
        """
        Load data from parquet and create document chunks.
        
        Returns:
            List of Document objects ready for embedding
        """
        print(f"Loading data from {self.parquet_path}...")
        df = pd.read_parquet(self.parquet_path)
        
        # Ensure required columns exist
        required_columns = ['job_posting_id', 'ai_summaries']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        documents = []
        
        print("Processing job postings...")
        for idx, row in tqdm(df.iterrows(), total=len(df)):
            job_id = row['job_posting_id']
            ai_summary = row['ai_summaries']
            
            # Skip if either field is null
            if pd.isna(job_id) or pd.isna(ai_summary):
                continue
            
            # Format the text for embedding
            formatted_text = self._format_job_for_embedding(job_id, ai_summary)
            
            # Create document with metadata
            doc = Document(
                page_content=formatted_text,
                metadata={
                    'job_posting_id': str(job_id),
                    'ai_summary': ai_summary if isinstance(ai_summary, str) else json.dumps(ai_summary)
                }
            )
            documents.append(doc)
        
        print(f"Created {len(documents)} documents for embedding")
        return documents
    
    def embed_and_store(self, documents: List[Document], batch_size: int = 100):
        """
        Generate embeddings and store in Qdrant.
        
        Args:
            documents: List of documents to embed
            batch_size: Number of documents to process at once
        """
        print("Generating embeddings and storing in Qdrant...")
        
        total_docs = len(documents)
        for i in tqdm(range(0, total_docs, batch_size)):
            batch = documents[i:i + batch_size]
            
            # Generate embeddings for batch
            texts = [doc.page_content for doc in batch]
            embeddings_batch = self.embeddings.embed_documents(texts)
            
            # Prepare points for Qdrant
            points = []
            for j, (doc, embedding) in enumerate(zip(batch, embeddings_batch)):
                point_id = str(uuid.uuid4())
                
                # Extract skills from the AI summary
                try:
                    summary = json.loads(doc.metadata['ai_summary']) if isinstance(doc.metadata['ai_summary'], str) else doc.metadata['ai_summary']
                    skills = summary.get('requirements', {}).get('skills', [])
                except (json.JSONDecodeError, TypeError):
                    skills = []
                
                points.append(
                    PointStruct(
                        id=point_id,
                        vector=embedding,
                        payload={
                            'job_posting_id': doc.metadata['job_posting_id'],
                            'text': doc.page_content,
                            'skills': skills
                        }
                    )
                )
            
            # Upload to Qdrant
            self.qdrant_client.upsert(
                collection_name=self.collection_name,
                points=points
            )
        
        print(f"Successfully embedded and stored {total_docs} job postings")
    
    def run(self, batch_size: int = 100):
        """
        Run the complete embedding pipeline.
        
        Args:
            batch_size: Number of documents to process at once
        """
        # Load and process data
        documents = self.load_and_process_data()
        
        if not documents:
            print("No documents to process")
            return
        
        # Embed and store
        self.embed_and_store(documents, batch_size=batch_size)
        
        # Print collection info
        collection_info = self.qdrant_client.get_collection(self.collection_name)
        print(f"\nCollection '{self.collection_name}' info:")
        print(f"  - Total vectors: {collection_info.vectors_count}")
        print(f"  - Status: {collection_info.status}")


def main():
    """Main function to run the embedding pipeline."""
    # Check for required environment variables
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY not found in environment variables. Please set it in your .env file.")
    
    if not os.path.exists(PARQUET_FILE_PATH):
        raise FileNotFoundError(f"Parquet file not found at {PARQUET_FILE_PATH}. Please update PARQUET_FILE_PATH in .env file.")
    
    print(f"Configuration:")
    print(f"  - Parquet file: {PARQUET_FILE_PATH}")
    print(f"  - Qdrant URL: {QDRANT_URL}")
    print(f"  - Collection: {COLLECTION_NAME}")
    print(f"  - Embedding model: {EMBEDDING_MODEL}")
    print(f"  - Vector size: {VECTOR_SIZE}")
    print()
    
    # Initialize and run pipeline
    pipeline = JobEmbeddingPipeline(
        parquet_path=PARQUET_FILE_PATH,
        qdrant_url=QDRANT_URL,
        qdrant_api_key=QDRANT_API_KEY,
        collection_name=COLLECTION_NAME
    )
    
    # Run the pipeline
    batch_size = int(os.getenv("BATCH_SIZE", 100))
    pipeline.run(batch_size=batch_size)


if __name__ == "__main__":
    main()