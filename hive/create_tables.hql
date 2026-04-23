CREATE DATABASE IF NOT EXISTS ${hiveconf:db};
USE ${hiveconf:db};

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:parsed_table} (
  candidate_id STRING,
  name STRING,
  email STRING,
  phone STRING,
  skills STRING,
  years_experience INT,
  source_file STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hiveconf:parsed_dir}';

SELECT COUNT(1) AS total_candidates FROM ${hiveconf:parsed_table};
