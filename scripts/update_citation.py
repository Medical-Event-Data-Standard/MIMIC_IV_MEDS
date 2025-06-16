#!/usr/bin/env python3
"""Update CITATION.cff with latest GitHub release."""

import json
import re
import urllib.request
from pathlib import Path


def main():
    # Find citation file
    script_dir = Path(__file__).parent
    citation_file = script_dir.parent / "CITATION.cff"

    # Get latest release
    url = "https://api.github.com/repos/Medical-Event-Data-Standard/MIMIC_IV_MEDS/releases/latest"
    with urllib.request.urlopen(url) as r:
        data = json.loads(r.read().decode())

    version = data["tag_name"].lstrip("v")
    date = data["published_at"].split("T")[0]

    # Update file
    content = citation_file.read_text()
    content = re.sub(r'version: "[^"]*"', f'version: "{version}"', content)
    content = re.sub(r"date-released: \d{4}-\d{2}-\d{2}", f"date-released: {date}", content)
    citation_file.write_text(content)

    print(f"✅ Updated: v{version} ({date})")


if __name__ == "__main__":
    main()
