import os
import json
import uuid
import logging
from datetime import datetime
from typing import List, Dict, Any, Tuple

from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PointStruct,
    OptimizersConfigDiff,
    HnswConfigDiff,
)


# -----------------------------
# Configuration
# -----------------------------
load_dotenv()

LOGGER = logging.getLogger("retrieval")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "jobs_fixed_chunks")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")

JOBS_FILE = os.getenv("JOBS_FILE", "jobs_summaries.json")
CHUNK_SIZE_CHARS = int(os.getenv("CHUNK_SIZE_CHARS", "5000"))
CHUNK_OVERLAP_CHARS = int(os.getenv("CHUNK_OVERLAP_CHARS", "500"))

# Qdrant connectivity tuning
QDRANT_TIMEOUT = float(os.getenv("QDRANT_TIMEOUT", "45"))  # seconds
QDRANT_PREFER_GRPC = os.getenv("QDRANT_PREFER_GRPC", "true").lower() == "true"
DEFAULT_LIMIT_CHUNKS = int(os.getenv("QDRANT_LIMIT_CHUNKS", "30"))


# -----------------------------
# Utilities
# -----------------------------
def _normalize_key(name: str) -> str:
    return name.strip().lower().replace(" ", "").replace("_", "")


def _pick(d: Dict[str, Any], candidates: List[str], default: Any = None) -> Any:
    """Pick first existing key from candidates with tolerant key matching (case/space/underscore insensitive)."""
    if not isinstance(d, dict):
        return default
    # Fast exact match first
    for key in candidates:
        if key in d and d[key] not in (None, ""):
            return d[key]
    # Tolerant match
    normalized_map = {_normalize_key(k): k for k in d.keys()}
    for key in candidates:
        k_norm = _normalize_key(key)
        if k_norm in normalized_map:
            val = d.get(normalized_map[k_norm])
            if val not in (None, ""):
                return val
    return default

def _require_env(value: str, name: str) -> str:
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def build_qdrant_client(prefer_grpc: bool | None = None, timeout: float | None = None) -> QdrantClient:
    """Create Qdrant Cloud client using URL + API key."""
    _require_env(QDRANT_URL, "QDRANT_URL")
    _require_env(QDRANT_API_KEY, "QDRANT_API_KEY")
    client = QdrantClient(
        url=QDRANT_URL,
        api_key=QDRANT_API_KEY,
        prefer_grpc=QDRANT_PREFER_GRPC if prefer_grpc is None else prefer_grpc,
        timeout=QDRANT_TIMEOUT if timeout is None else timeout,
    )
    return client


def ensure_collection(client: QdrantClient, vector_size: int = 1536) -> None:
    """Create collection if it does not exist."""
    try:
        collections = client.get_collections().collections
        existing = {c.name for c in collections}
    except Exception:
        # If listing fails (permissions or new cluster), try to create directly
        existing = set()

    if COLLECTION_NAME in existing:
        LOGGER.info(f"Collection {COLLECTION_NAME} already exists")
        return

    LOGGER.info(f"Creating collection: {COLLECTION_NAME}")
    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(
            size=vector_size,
            distance=Distance.COSINE,
        ),
        optimizers_config=OptimizersConfigDiff(
            default_segment_number=2,
            indexing_threshold=10000,
        ),
        hnsw_config=HnswConfigDiff(m=16, ef_construct=100),
    )


def flatten_job_summary(job: Dict[str, Any]) -> Tuple[str, str, str]:
    """Create a stable text representation from heterogeneous job summary JSON.

    Supports both styles in your jobs_summaries.json, e.g.
    - "Job Information" → {"Job Title", "Job Level", "Work Type"}
    - "Requirements" → {"Skills", "Experience"/"Years_min"..., "Qualifications"}

    Returns: (job_id, job_title, text)
    """
    job_id = str(_pick(job, ["job_id", "id"]) or uuid.uuid4())

    info = _pick(job, ["Job Information", "job_info"], {}) or {}
    job_title = _pick(info, ["job_title", "Job Title"]) or _pick(job, ["job_title", "title"], "Unknown")
    job_level = _pick(info, ["job_level", "Job Level"], "")
    work_type = _pick(info, ["work_type", "Work Type"], _pick(job, ["workplace_type"], ""))

    comp = _pick(job, ["Compensation", "compensation"], {}) or {}
    salary = _pick(comp, ["salary"], {}) or {}
    min_salary = _pick(salary, ["min", "Min", "Min Salary", "min_salary"]) or _pick(comp, ["Min Salary", "min_salary"])
    max_salary = _pick(salary, ["max", "Max", "Max Salary", "max_salary"]) or _pick(comp, ["Max Salary", "max_salary"])

    overview = _pick(job, ["Overview", "overview"], "") or ""
    responsibilities = _pick(job, ["Responsibilities", "responsibilities"], []) or []
    if isinstance(responsibilities, list):
        responsibilities_text = "\n- " + "\n- ".join(map(str, responsibilities)) if responsibilities else ""
    else:
        responsibilities_text = str(responsibilities)

    requirements = _pick(job, ["Requirements", "requirements"], {}) or {}
    # Skills can appear under simplified or detailed schema
    skills = _pick(requirements, ["Skills", "skills", "required.skills"], []) or []
    if not skills and isinstance(requirements.get("required"), dict):
        skills = _pick(requirements.get("required"), ["skills"], []) or []
    if not isinstance(skills, list):
        skills = [str(skills)]

    # Experience variations (Years_min/years_min/Years Min...)
    exp_src = _pick(requirements, ["Experience", "experience"], {}) or {}
    if not exp_src and isinstance(requirements.get("required"), dict):
        exp_src = _pick(requirements.get("required"), ["experience"], {}) or {}
    years_min = _pick(exp_src, ["years_min", "Years_min", "Years Min", "YearsMin"])
    level = _pick(exp_src, ["level", "Level"]) or _pick(info, ["job_level", "Job Level"], "")
    experience = {"years_min": years_min, "level": level}

    qualifications = _pick(requirements, ["Qualifications", "qualifications", "required.qualifications"], []) or []
    if not qualifications and isinstance(requirements.get("required"), dict):
        qualifications = _pick(requirements.get("required"), ["qualifications"], []) or []
    if not isinstance(qualifications, list):
        qualifications = [str(qualifications)]

    sections = [
        f"Job Title: {job_title}",
        f"Job Level: {job_level}",
        f"Work Type: {work_type}",
        f"Salary Min: {min_salary}",
        f"Salary Max: {max_salary}",
        "Overview:",
        str(overview),
        "Responsibilities:",
        responsibilities_text,
        "Required Skills:",
        ", ".join(map(str, skills)),
        "Experience:",
        json.dumps(experience, ensure_ascii=False),
        "Qualifications:",
        "\n- " + "\n- ".join(map(str, qualifications)) if qualifications else "",
    ]

    text = "\n".join([s for s in sections if s is not None])
    return job_id, str(job_title), text


def chunk_text_sliding_window(text: str, size: int, overlap: int) -> List[Tuple[str, int]]:
    """Fixed-size sliding window chunking by characters.

    Returns list of (chunk_text, start_offset)
    """
    if size <= 0:
        raise ValueError("chunk size must be > 0")
    if overlap < 0 or overlap >= size:
        raise ValueError("overlap must be >= 0 and < chunk size")

    chunks: List[Tuple[str, int]] = []
    start = 0
    text_len = len(text)
    while start < text_len:
        end = min(start + size, text_len)
        chunks.append((text[start:end], start))
        if end == text_len:
            break
        start = end - overlap
    return chunks


def embed_texts(texts: List[str]) -> List[List[float]]:
    _require_env(OPENAI_API_KEY, "OPENAI_API_KEY")
    embeddings = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY, model=EMBEDDING_MODEL)
    return embeddings.embed_documents(texts)


def index_jobs_file(jobs_file: str = JOBS_FILE) -> Dict[str, Any]:
    """Index jobs_summaries.json into Qdrant Cloud with fixed-size sliding window chunks."""
    client = build_qdrant_client()
    ensure_collection(client, vector_size=1536)

    # Load
    with open(jobs_file, "r", encoding="utf-8") as f:
        jobs = json.load(f)

    if not isinstance(jobs, list):
        raise ValueError("jobs_summaries.json must be a JSON array")

    LOGGER.info(f"Loaded {len(jobs)} jobs from {jobs_file}")

    # Build chunks in memory to minimize repeated embeddings calls
    all_points: List[PointStruct] = []
    total_chunks = 0

    for job in jobs:
        job_id, job_title, text = flatten_job_summary(job)
        chunks = chunk_text_sliding_window(text, CHUNK_SIZE_CHARS, CHUNK_OVERLAP_CHARS)
        if not chunks:
            continue

        chunk_texts = [c[0] for c in chunks]
        vectors = embed_texts(chunk_texts)

        for (chunk_text, start_offset), vector in zip(chunks, vectors):
            point = PointStruct(
                id=str(uuid.uuid4()),
                vector=vector,
                payload={
                    "doc_type": "job_summary",
                    "job_id": job_id,
                    "job_title": job_title,
                    "external_id": f"job_{job_id}_offset_{start_offset}",
                    "chunk_index": total_chunks,  # global index for traceability
                    "offset_start": start_offset,
                    "source": jobs_file,
                    "created_date": datetime.utcnow().isoformat(),
                    "text": chunk_text,
                },
            )
            all_points.append(point)
            total_chunks += 1

    # Upsert in batches to avoid very large payloads
    BATCH = 256
    for i in range(0, len(all_points), BATCH):
        batch = all_points[i : i + BATCH]
        LOGGER.info(f"Upserting points {i}..{i+len(batch)-1} / {len(all_points)}")
        client.upsert(collection_name=COLLECTION_NAME, points=batch, wait=True)

    return {"jobs": len(jobs), "chunks": len(all_points), "collection": COLLECTION_NAME}


def resume_json_to_query_text(resume: Dict[str, Any]) -> str:
    """Flatten an uploaded resume summary JSON into a query text."""
    title = resume.get("title") or resume.get("job_title") or ""
    overview = resume.get("overview") or resume.get("summary") or ""
    skills = (
        resume.get("skills")
        or resume.get("Requirements", {}).get("skills")
        or resume.get("requirements", {}).get("skills")
        or []
    )
    if not isinstance(skills, list):
        skills = [str(skills)]
    experience = (
        resume.get("experience")
        or resume.get("Requirements", {}).get("experience")
        or resume.get("requirements", {}).get("experience")
        or {}
    )
    qualifications = (
        resume.get("qualifications")
        or resume.get("Requirements", {}).get("qualifications")
        or resume.get("requirements", {}).get("qualifications")
        or []
    )
    if not isinstance(qualifications, list):
        qualifications = [str(qualifications)]

    sections = [
        f"Title: {title}",
        str(overview),
        "Skills:", ", ".join(map(str, skills)),
        "Experience:", json.dumps(experience, ensure_ascii=False),
        "Qualifications:", "\n- " + "\n- ".join(map(str, qualifications)) if qualifications else "",
    ]
    return "\n".join([s for s in sections if s is not None])


def search_by_resume_json(
    resume_summary: Dict[str, Any],
    limit_chunks: int = DEFAULT_LIMIT_CHUNKS,
    top_k_docs: int = 5,
) -> Dict[str, Any]:
    """Search Qdrant using flattened resume summary JSON and aggregate to doc-level."""
    client = build_qdrant_client()

    # Prepare query embedding
    query_text = resume_json_to_query_text(resume_summary)
    embeddings = OpenAIEmbeddings(openai_api_key=_require_env(OPENAI_API_KEY, "OPENAI_API_KEY"), model=EMBEDDING_MODEL)
    query_vec = embeddings.embed_query(query_text)

    # Search in chunks
    selector = {"include": ["job_id", "job_title", "offset_start", "text"]}
    try:
        results = client.search(
            collection_name=COLLECTION_NAME,
            query_vector=query_vec,
            limit=limit_chunks,
            with_payload=selector,
        )
    except Exception as e:
        # Fallback: switch to REST and longer timeout, then retry once
        LOGGER.warning(f"Primary search failed ({e}); retrying with REST and extended timeout...")
        client = build_qdrant_client(prefer_grpc=False, timeout=max(QDRANT_TIMEOUT, 90.0))
        results = client.search(
            collection_name=COLLECTION_NAME,
            query_vector=query_vec,
            limit=limit_chunks,
            with_payload=selector,
        )

    # Aggregate to document level by job_id
    doc_scores: Dict[str, float] = {}
    doc_hits: Dict[str, List[Dict[str, Any]]] = {}
    for r in results:
        payload = r.payload or {}
        job_id = str(payload.get("job_id"))
        if not job_id:
            # Skip chunks without job identifier
            continue
        doc_scores[job_id] = max(doc_scores.get(job_id, float("-inf")), r.score)
        doc_hits.setdefault(job_id, []).append({
            "score": r.score,
            "text": payload.get("text", ""),
            "job_title": payload.get("job_title", ""),
            "offset_start": payload.get("offset_start", 0),
        })

    # Rank documents by best chunk score desc
    ranked = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)[:top_k_docs]
    top_docs = []
    for job_id, agg_score in ranked:
        hits = sorted(doc_hits[job_id], key=lambda x: x["score"], reverse=True)[:5]
        job_title = hits[0].get("job_title", "") if hits else ""
        top_docs.append({
            "job_id": job_id,
            "job_title": job_title,
            "aggregate_score": agg_score,
            "snippets": hits,
        })

    return {
        "query_preview": query_text[:500],
        "results": top_docs,
    }


if __name__ == "__main__":
    # Simple CLI helpers
    import argparse

    parser = argparse.ArgumentParser(description="Index and search jobs summaries with fixed-size chunking on Qdrant Cloud")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_index = sub.add_parser("index", help="Index jobs_summaries.json into Qdrant Cloud")
    p_index.add_argument("--file", default=JOBS_FILE, help="Path to jobs_summaries.json")

    p_search = sub.add_parser("search", help="Search by resume summary JSON file")
    p_search.add_argument("--resume", required=True, help="Path to resume summary JSON file")
    p_search.add_argument("--top", type=int, default=5, help="Top K documents")

    args = parser.parse_args()

    if args.cmd == "index":
        stats = index_jobs_file(args.file)
        print(json.dumps(stats, indent=2))
    elif args.cmd == "search":
        with open(args.resume, "r", encoding="utf-8") as f:
            resume_json = json.load(f)
        out = search_by_resume_json(resume_json, top_k_docs=args.top)
        print(json.dumps(out, indent=2, ensure_ascii=False))


