import json
import re
import sys


EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(\+?\d[\d\s().-]{7,}\d)")
YEARS_RE = re.compile(r"(\d+)\+?\s+years", re.IGNORECASE)

SKILL_KEYWORDS = [
    "python",
    "hadoop",
    "hive",
    "mapreduce",
    "solr",
    "spark",
    "java",
    "sql",
    "linux",
]


def extract_name(text):
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.lower().startswith("name:"):
            return line.split(":", 1)[1].strip()
        return line
    return "Unknown"


def extract_skills(text):
    lower = text.lower()
    found = []
    for skill in SKILL_KEYWORDS:
        if skill in lower:
            found.append(skill)
    return found


def extract_years(text):
    matches = YEARS_RE.findall(text)
    if not matches:
        return 0
    return max(int(m) for m in matches)


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        record = json.loads(line)
        text = record.get("text", "")
        email_match = EMAIL_RE.search(text)
        phone_match = PHONE_RE.search(text)
        email = email_match.group(0) if email_match else ""
        phone = phone_match.group(0) if phone_match else ""
        name = extract_name(text)
        skills = extract_skills(text)
        years = extract_years(text)
        source_file = record.get("file_name", "unknown")

        key = email or source_file
        payload = [
            key,
            name,
            email,
            phone,
            ",".join(skills),
            str(years),
            source_file,
        ]
        print("\t".join(payload))


if __name__ == "__main__":
    main()
