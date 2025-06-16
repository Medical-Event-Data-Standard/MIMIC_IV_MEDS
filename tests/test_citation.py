#!/usr/bin/env python3
import json
import re
import urllib.request
from pathlib import Path


def test_citation_file():
    """Test CITATION.cff file exists and has required fields."""
    citation_file = Path(__file__).parent.parent / "CITATION.cff"
    content = citation_file.read_text()

    required = ["cff-version", "message", "authors", "title", "version", "date-released", "url"]
    missing = [field for field in required if not re.search(f"^{field}:", content, re.MULTILINE)]

    assert not missing, f"Missing fields: {', '.join(missing)}"
    assert re.search(r"date-released: \d{4}-\d{2}-\d{2}", content), "Invalid date format"


def test_github_api():
    """Test that GitHub API returns valid data."""
    url = "https://api.github.com/repos/Medical-Event-Data-Standard/MIMIC_IV_MEDS/releases/latest"
    with urllib.request.urlopen(url) as r:
        data = json.loads(r.read().decode())

    assert "tag_name" in data, "Missing tag_name"
    assert "published_at" in data, "Missing published_at"

    version = data["tag_name"].lstrip("v")
    date = data["published_at"].split("T")[0]

    assert re.match(r"\d+\.\d+\.\d+", version), f"Invalid version: {version}"
    assert re.match(r"\d{4}-\d{2}-\d{2}", date), f"Invalid date: {date}"
