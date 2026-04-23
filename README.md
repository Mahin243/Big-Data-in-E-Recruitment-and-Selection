# Big Data Resume Framework (Local Single-Node Demo)

This project implements a deterministic, rule-based resume pipeline using the Hadoop ecosystem:
HDFS ingestion → Apache Tika extraction → MapReduce parsing → Hive tables → Solr search → weighted scoring.

## Prerequisites (local installs, no Docker)
1. **Java 8+** in `PATH`
2. **Hadoop** (single-node) with `HADOOP_HOME` set and `hdfs`/`hadoop` in `PATH`
3. **Hive** in `PATH` (or update `configs/config.yaml`)
4. **Solr** in `PATH` (or update `configs/config.yaml`)
5. **Apache Tika Server** running on `http://localhost:9998`

> Tika server example:
> `java -jar tika-server-standard-2.9.2.jar --port 9998`

## Setup
```bash
pip install -r requirements.txt
```

Update `configs/config.yaml` for custom paths or endpoints if needed.

## Start services
Ensure HDFS is running and writable, then start Solr and Tika Server. Update `configs/config.yaml` if your binaries are not in `PATH`.

## Run the full pipeline
```powershell
.\scripts\run_pipeline.ps1
```

## Manual step-by-step
```bash
python scripts/ingest_hdfs.py
python scripts/extract_tika.py
python scripts/run_mapreduce.py
python scripts/run_hive.py
python scripts/solr_setup.py
python scripts/solr_index.py
```

## Search candidates (Solr)
```bash
python scripts/solr_search.py --q "skills:(python AND hadoop)"
```

## Score candidates (Weighted Scoring Model)
```bash
python scripts/score_candidates.py --q "*:*"
```

Adjust scoring criteria and weights in `configs/scoring.yaml`.
