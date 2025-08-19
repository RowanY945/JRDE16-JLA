import os
from typing import List, Dict, Any

from dotenv import load_dotenv
from langchain_openai import ChatOpenAI


load_dotenv()


def generate_match_explanation(
    resume_summary: Dict[str, Any],
    retrieved_docs: List[Dict[str, Any]],
    model: str = os.getenv("GENERATION_MODEL", "gpt-4o-mini"),
    temperature: float = float(os.getenv("GENERATION_TEMPERATURE", "0.2")),
) -> str:
    """Given a resume summary JSON and retrieved job docs, produce a brief explanation.

    Returns markdown string.
    """
    llm = ChatOpenAI(model=model, temperature=temperature)

    # Build a concise prompt
    resume_preview = {
        "title": resume_summary.get("title") or resume_summary.get("job_title"),
        "skills": resume_summary.get("skills")
                  or resume_summary.get("Requirements", {}).get("skills")
                  or resume_summary.get("requirements", {}).get("skills"),
        "experience": resume_summary.get("experience")
                     or resume_summary.get("Requirements", {}).get("experience")
                     or resume_summary.get("requirements", {}).get("experience"),
        "overview": resume_summary.get("overview") or resume_summary.get("summary"),
    }

    lines = [
        "You are a helpful assistant that explains why the retrieved jobs match the resume.",
        "Write in concise bullet points, avoid repetition, and be specific.",
        "",
        "Resume Summary:",
        str(resume_preview),
        "",
        "Retrieved Jobs (top results):",
    ]

    for i, d in enumerate(retrieved_docs, 1):
        lines.append(f"{i}. job_id={d.get('job_id')} | title={d.get('job_title')} | score={d.get('aggregate_score'):.3f}")
        snips = d.get("snippets", [])
        for s in snips[:2]:
            snippet = s.get("text", "")
            snippet = snippet[:350].replace("\n", " ")
            lines.append(f"   - snippet: {snippet}")

    lines += [
        "",
        "Task:",
        "- Explain key overlaps (skills, experience level, responsibilities).",
        "- Mention gaps if any (e.g., missing tools).",
        "- Output in markdown bullets with short sentences.",
    ]

    prompt = "\n".join(lines)
    response = llm.invoke(prompt)
    try:
        return response.content  # AIMessage
    except AttributeError:
        return str(response)


