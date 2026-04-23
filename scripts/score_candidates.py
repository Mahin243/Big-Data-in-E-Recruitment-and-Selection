import argparse
import json

import requests
import yaml

from common import load_config, resolve_path


def load_scoring():
    with open(resolve_path("configs/scoring.yaml"), "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def score_candidate(candidate, scoring):
    weights = scoring["weights"]
    required_skills = scoring["skills"]["required"]
    target_years = scoring["experience"]["target_years"]
    keywords = scoring.get("keywords", [])

    skills_raw = candidate.get("skills", [])
    if isinstance(skills_raw, str):
        skills_raw = [s for s in skills_raw.split(",") if s]
    skills = [s.lower() for s in skills_raw]
    skill_matches = sum(1 for s in required_skills if s.lower() in skills)
    skill_score = (
        (skill_matches / len(required_skills)) * weights["skills"]
        if required_skills
        else 0
    )

    years = int(candidate.get("years_experience") or 0)
    years_score = min(years / max(target_years, 1), 1) * weights["years_experience"]

    combined_text = f"{candidate.get('name', '')} {' '.join(skills)}".lower()
    keyword_bonus = weights["keyword_bonus"] if any(k in combined_text for k in keywords) else 0

    total = round(skill_score + years_score + keyword_bonus, 2)
    return total


def main():
    parser = argparse.ArgumentParser(description="Score candidates from Solr results.")
    parser.add_argument("--q", default="*:*", help="Solr query for candidate selection.")
    parser.add_argument("--rows", type=int, default=100, help="Max candidates to score.")
    args = parser.parse_args()

    config = load_config()
    base_url = config["solr"]["base_url"]
    core = config["solr"]["core"]
    scoring = load_scoring()

    resp = requests.get(
        f"{base_url}/{core}/select",
        params={"q": args.q, "rows": args.rows, "wt": "json"},
        timeout=10,
    )
    resp.raise_for_status()
    docs = resp.json().get("response", {}).get("docs", [])

    ranked = []
    for doc in docs:
        score = score_candidate(doc, scoring)
        ranked.append({**doc, "score": score})

    ranked.sort(key=lambda d: d["score"], reverse=True)
    print(json.dumps(ranked, indent=2))


if __name__ == "__main__":
    main()
