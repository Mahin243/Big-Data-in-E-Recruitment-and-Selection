from common import ensure_dir, error, load_config, resolve_path, run


def main():
    config = load_config()
    local_dir = resolve_path(config["local"]["resumes_dir"])
    hdfs_raw = config["hdfs"]["raw_dir"]
    hdfs_bin = config["hadoop"]["hdfs_bin"]

    if not local_dir.exists():
        error(f"Local resumes directory not found: {local_dir}")
        raise SystemExit(1)

    ensure_dir(resolve_path(config["local"]["staging_dir"]))

    run(f'{hdfs_bin} dfs -mkdir -p "{hdfs_raw}"')

    files = sorted([p for p in local_dir.rglob("*") if p.is_file()])
    if not files:
        error(f"No resume files found in {local_dir}")
        raise SystemExit(1)

    for path in files:
        run(f'{hdfs_bin} dfs -put -f "{path}" "{hdfs_raw}/"')

    print(f"Ingested {len(files)} files into {hdfs_raw}")


if __name__ == "__main__":
    main()
