import requests

from common import load_config, read_tsv, resolve_path


def main():
    config = load_config()
    base_url = config["solr"]["base_url"]
    core = config["solr"]["core"]
    parsed_tsv = resolve_path(config["local"]["parsed_tsv"])

    docs = []
    for row in read_tsv(parsed_tsv):
        if len(row) < 7:
            continue
        candidate_id, name, email, phone, skills, years, source_file = row[:7]
        docs.append(
            {
                "id": candidate_id,
                "candidate_id": candidate_id,
                "name": name,
                "email": email,
                "phone": phone,
                "skills": [s for s in skills.split(",") if s],
                "years_experience": int(years or 0),
                "source_file": source_file,
            }
        )

    if not docs:
        raise SystemExit("No documents found to index.")

    resp = requests.post(
        f"{base_url}/{core}/update",
        params={"commit": "true"},
        json=docs,
        timeout=10,
    )
    resp.raise_for_status()
    print(f"Indexed {len(docs)} documents into Solr core '{core}'.")


if __name__ == "__main__":
    main()
