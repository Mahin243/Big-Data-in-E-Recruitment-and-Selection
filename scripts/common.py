import glob
import json
import os
import subprocess
import sys
from pathlib import Path

import yaml


ROOT_DIR = Path(__file__).resolve().parents[1]


def load_config():
    config_path = ROOT_DIR / "configs" / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        raw = f.read()
    expanded = os.path.expandvars(raw)
    return yaml.safe_load(expanded)


def run(cmd, cwd=None):
    result = subprocess.run(cmd, cwd=cwd, shell=True, text=True, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {cmd}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result.stdout


def ensure_dir(path):
    Path(path).mkdir(parents=True, exist_ok=True)

def resolve_path(path_str):
    path = Path(path_str)
    return path if path.is_absolute() else ROOT_DIR / path


def find_streaming_jar(pattern):
    matches = glob.glob(pattern)
    if not matches:
        raise FileNotFoundError(f"No streaming jar found for pattern: {pattern}")
    return sorted(matches)[-1]


def read_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def write_jsonl(path, records):
    with open(path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=True) + "\n")


def read_tsv(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            yield line.split("\t")


def error(msg):
    print(msg, file=sys.stderr)
