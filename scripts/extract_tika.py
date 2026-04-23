import datetime as dt

import requests

from common import ensure_dir, error, load_config, resolve_path, run, write_jsonl


def extract_text(tika_url, file_path, timeout):
    with open(file_path, "rb") as f:
        resp = requests.put(
            f"{tika_url}/tika",
            data=f,
            headers={"Accept": "text/plain"},
            timeout=timeout,
        )
    resp.raise_for_status()
    return resp.text


def main():
    config = load_config()
    hdfs_bin = config["hadoop"]["hdfs_bin"]
    hdfs_raw = config["hdfs"]["raw_dir"]
    hdfs_extracted = config["hdfs"]["extracted_dir"]
    staging_dir = resolve_path(config["local"]["staging_dir"])
    extracted_jsonl = resolve_path(config["local"]["extracted_jsonl"])

    ensure_dir(staging_dir)
    ensure_dir(extracted_jsonl.parent)

    run(f'{hdfs_bin} dfs -get -f "{hdfs_raw}/*" "{staging_dir}"')

    files = sorted([p for p in staging_dir.rglob("*") if p.is_file()])
    if not files:
        error(f"No files staged from HDFS path {hdfs_raw}")
        raise SystemExit(1)

    records = []
    for path in files:
        text = extract_text(
            config["tika"]["server_url"], path, config["tika"]["timeout_sec"]
        )
        records.append(
            {
                "file_name": path.name,
                "source_path": str(path),
                "extracted_at": dt.datetime.utcnow().isoformat() + "Z",
                "text": text,
            }
        )

    write_jsonl(extracted_jsonl, records)

    run(f'{hdfs_bin} dfs -mkdir -p "{hdfs_extracted}"')
    run(f'{hdfs_bin} dfs -put -f "{extracted_jsonl}" "{hdfs_extracted}/"')

    print(f"Extracted {len(records)} resumes to {hdfs_extracted}")


if __name__ == "__main__":
    main()
