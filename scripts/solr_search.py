import argparse
import json

import requests

from common import load_config


def main():
    parser = argparse.ArgumentParser(description="Search candidates via Solr.")
    parser.add_argument("--q", required=True, help="Solr query string (supports boolean).")
    parser.add_argument("--rows", type=int, default=20, help="Number of rows to return.")
    args = parser.parse_args()

    config = load_config()
    base_url = config["solr"]["base_url"]
    core = config["solr"]["core"]

    resp = requests.get(
        f"{base_url}/{core}/select",
        params={"q": args.q, "rows": args.rows, "wt": "json"},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    print(json.dumps(data.get("response", {}), indent=2))


if __name__ == "__main__":
    main()
