$ErrorActionPreference = "Stop"

python scripts\ingest_hdfs.py
python scripts\extract_tika.py
python scripts\run_mapreduce.py
python scripts\run_hive.py
python scripts\solr_setup.py
python scripts\solr_index.py
