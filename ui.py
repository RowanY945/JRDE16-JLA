import os
import json
import tempfile
from typing import Dict, Any

import streamlit as st
from dotenv import load_dotenv

from retrieval import index_jobs_file, search_by_resume_json
from generation import generate_match_explanation


load_dotenv()

st.set_page_config(page_title="RAG - Jobs Matcher (Streamlit)", layout="wide")


def sidebar_env_info():
    with st.sidebar:
        st.markdown("**Environment**")
        st.write("QDRANT_URL:", os.getenv("QDRANT_URL", "(not set)"))
        st.write("COLLECTION_NAME:", os.getenv("COLLECTION_NAME", "jobs_fixed_chunks"))
        st.write("EMBEDDING_MODEL:", os.getenv("EMBEDDING_MODEL", "text-embedding-3-small"))
        st.write("CHUNK_SIZE_CHARS:", os.getenv("CHUNK_SIZE_CHARS", "2500"))
        st.write("CHUNK_OVERLAP_CHARS:", os.getenv("CHUNK_OVERLAP_CHARS", "500"))


def page_index():
    st.header("Index - Build/Refresh Qdrant Collection")
    st.caption("Rebuild Cloud Qdrant index from a jobs_summaries.json using fixed-size sliding window chunking.")

    default_path = os.getenv("JOBS_FILE", "jobs_summaries.json")
    col1, col2 = st.columns([3, 2])
    with col1:
        file_path = st.text_input("Path to jobs_summaries.json", value=default_path)
    with col2:
        uploaded = st.file_uploader("Or upload jobs_summaries.json", type=["json"], accept_multiple_files=False)

    run = st.button("Run Index", type="primary")
    out = st.empty()

    if run:
        try:
            if uploaded is not None:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp:
                    tmp.write(uploaded.read())
                    tmp_path = tmp.name
                stats = index_jobs_file(tmp_path)
                os.unlink(tmp_path)
            else:
                stats = index_jobs_file(file_path)
            out.code(json.dumps(stats, indent=2), language="json")
            st.success("Index completed")
        except Exception as e:
            st.error(f"Index error: {e}")


def page_search():
    st.header("Search - Match Resume Summary to Jobs")
    st.caption("Paste or upload a resume summary JSON. We'll retrieve top matching jobs and generate an explanation.")

    col1, col2 = st.columns([3, 2])
    with col1:
        resume_text = st.text_area(
            "Resume Summary JSON",
            height=240,
            placeholder='{"title": "Data Engineer", "skills": ["Python", "AWS"], "overview": "..."}',
        )
    with col2:
        resume_upload = st.file_uploader("Or upload resume JSON", type=["json"], accept_multiple_files=False)

    top_k = st.slider("Top K Documents", min_value=1, max_value=10, value=5, step=1)
    search_btn = st.button("Search & Explain", type="primary")

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    # Chat display area
    for role, content in st.session_state.chat_history:
        with st.chat_message(role):
            st.markdown(content)

    if search_btn:
        # Determine resume JSON
        resume: Dict[str, Any]
        if resume_upload is not None:
            try:
                resume = json.loads(resume_upload.read().decode("utf-8"))
            except Exception as e:
                st.error(f"Invalid uploaded JSON: {e}")
                return
        else:
            try:
                resume = json.loads(resume_text)
            except Exception as e:
                st.error(f"Invalid JSON in text area: {e}")
                return

        # Append user message
        st.session_state.chat_history.append(("user", f"Resume JSON submitted. Top K = {top_k}"))
        with st.chat_message("assistant"):
            with st.spinner("Searching and generating explanation..."):
                try:
                    out = search_by_resume_json(resume, top_k_docs=top_k)
                    results_json = json.dumps(out, indent=2, ensure_ascii=False)
                    explanation_md = generate_match_explanation(resume, out.get("results", []))

                    content = (
                        f"### Retrieved Results\n\n" +
                        f"```json\n{results_json}\n```\n\n" +
                        f"---\n\n### Explanation\n\n{explanation_md}"
                    )
                    st.markdown(content)
                    st.session_state.chat_history.append(("assistant", content))
                except Exception as e:
                    st.error(f"Search error: {e}")

    # Chat input to allow quick reruns with pasted JSON
    user_msg = st.chat_input("Paste resume summary JSON to run search")
    if user_msg:
        st.session_state.chat_history.append(("user", "Resume JSON submitted via chat input."))
        with st.chat_message("assistant"):
            with st.spinner("Searching and generating explanation..."):
                try:
                    resume = json.loads(user_msg)
                    out = search_by_resume_json(resume, top_k_docs=top_k)
                    results_json = json.dumps(out, indent=2, ensure_ascii=False)
                    explanation_md = generate_match_explanation(resume, out.get("results", []))
                    content = (
                        f"### Retrieved Results\n\n" +
                        f"```json\n{results_json}\n```\n\n" +
                        f"---\n\n### Explanation\n\n{explanation_md}"
                    )
                    st.markdown(content)
                    st.session_state.chat_history.append(("assistant", content))
                except Exception as e:
                    st.error(f"Invalid JSON: {e}")


def main():
    st.title("RAG - Jobs Fixed Chunking with Qdrant Cloud (Streamlit)")
    sidebar_env_info()

    mode = st.sidebar.radio("Mode", ["Search", "Index"], index=0)
    if mode == "Index":
        page_index()
    else:
        page_search()


if __name__ == "__main__":
    main()


