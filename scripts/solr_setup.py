import json

import requests

from common import load_config, resolve_path, run


def core_exists(base_url, core):
    resp = requests.get(
        f"{base_url}/admin/cores",
        params={"action": "STATUS", "core": core},
        timeout=10,
    )
    resp.raise_for_status()
    status = resp.json().get("status", {})
    return core in status


def main():
    config = load_config()
    base_url = config["solr"]["base_url"]
    core = config["solr"]["core"]

    if not core_exists(base_url, core):
        solr_bin = config["solr"]["solr_bin"]
        run(f'{solr_bin} create -c "{core}"')

    schema_path = resolve_path("configs/solr_schema.json")
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_payload = json.load(f)

    fields_resp = requests.get(f"{base_url}/{core}/schema/fields", timeout=10)
    fields_resp.raise_for_status()
    existing = {f["name"] for f in fields_resp.json().get("fields", [])}

    add_fields = [f for f in schema_payload.get("add-field", []) if f["name"] not in existing]
    if add_fields:
        resp = requests.post(
            f"{base_url}/{core}/schema",
            json={"add-field": add_fields},
            timeout=10,
        )
        resp.raise_for_status()

    print(f"Solr core '{core}' ready.")


if __name__ == "__main__":
    main()
