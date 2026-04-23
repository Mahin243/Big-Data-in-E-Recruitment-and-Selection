import os

from common import find_streaming_jar, load_config, resolve_path, run


def main():
    config = load_config()
    hdfs_bin = config["hadoop"]["hdfs_bin"]
    hadoop_bin = config["hadoop"]["hadoop_bin"]
    extracted_dir = config["hdfs"]["extracted_dir"]
    parsed_dir = config["hdfs"]["parsed_dir"]

    streaming_jar = find_streaming_jar(config["hadoop"]["streaming_jar_glob"])
    mapper = resolve_path("scripts/mapreduce/mapper.py")
    reducer = resolve_path("scripts/mapreduce/reducer.py")

    run(f'{hdfs_bin} dfs -rm -r -f "{parsed_dir}"')

    cmd = (
        f'{hadoop_bin} jar "{streaming_jar}" '
        f'-input "{extracted_dir}/*" '
        f'-output "{parsed_dir}" '
        f'-mapper "python {mapper}" '
        f'-reducer "python {reducer}" '
        f'-file "{mapper}" -file "{reducer}"'
    )
    run(cmd)

    local_parsed = resolve_path(config["local"]["parsed_tsv"])
    local_parsed.parent.mkdir(parents=True, exist_ok=True)
    run(f'{hdfs_bin} dfs -getmerge "{parsed_dir}" "{local_parsed}"')

    print(f"MapReduce output merged to {local_parsed}")


if __name__ == "__main__":
    os.environ["PYTHONIOENCODING"] = "utf-8"
    main()
