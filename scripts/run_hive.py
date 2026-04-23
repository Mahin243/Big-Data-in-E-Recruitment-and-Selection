from common import load_config, resolve_path, run


def main():
    config = load_config()
    hive_bin = config["hive"]["hive_bin"]
    hive_script = resolve_path("hive/create_tables.hql")
    cmd = (
        f'{hive_bin} '
        f'-hiveconf db={config["hive"]["database"]} '
        f'-hiveconf parsed_table={config["hive"]["parsed_table"]} '
        f'-hiveconf parsed_dir={config["hdfs"]["parsed_dir"]} '
        f'-f "{hive_script}"'
    )
    run(cmd)
    print("Hive tables created or updated.")


if __name__ == "__main__":
    main()
