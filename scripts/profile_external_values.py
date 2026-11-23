"""Profile the external values of a MEDS root: how many, how big, and how fast each encoding reads.

    uv run --extra external-values python scripts/profile_external_values.py <meds_root> [--sample 200]

Reads up to `--sample` links per encoding (a seeded random sample) with
`MIMIC_IV_MEDS.multimodal.external_values.read`, the reference reader for each encoding, and prints one
line per encoding: links, files and bytes on disk (all links), and read time per object (the sample).
Prints counts and timings only, never a link or a value, so it is safe to run on credentialed data.
"""

from __future__ import annotations

import argparse
import random
import statistics
import time
from pathlib import Path

import polars as pl

from MIMIC_IV_MEDS.multimodal import external_values as ev


def profile(meds_root: Path, sample: int, seed: int = 0) -> list[dict]:
    links = (
        pl.scan_parquet(meds_root / "data" / "**" / "*.parquet")
        .filter(pl.col(ev.LINK_COLUMN).is_not_null())
        .select(ev.LINK_COLUMN, ev.ENCODING_COLUMN)
        .unique()
        .collect()
        .sort(ev.LINK_COLUMN)
    )
    rows = []
    for (encoding,), group in links.group_by(ev.ENCODING_COLUMN, maintain_order=True):
        all_links = group[ev.LINK_COLUMN].to_list()
        files = [f for link in all_links for f in ev.files_of(meds_root, link, encoding)]
        picked = random.Random(seed).sample(all_links, min(sample, len(all_links)))
        times = []
        for link in picked:
            t0 = time.perf_counter()
            obj = ev.read(meds_root, link, encoding)
            if encoding == "jpeg":
                obj.load()  # PIL opens lazily
            times.append(time.perf_counter() - t0)
        size = sum(f.stat().st_size for f in files)
        rows.append({
            "encoding": encoding,
            "links": len(all_links),
            "files": len(files),
            "MB": round(size / 1e6, 1),
            "MB_per_link": round(size / 1e6 / len(all_links), 3),
            "sampled": len(picked),
            "read_ms_median": round(1e3 * statistics.median(times), 1),
            "read_ms_p90": round(1e3 * sorted(times)[int(0.9 * (len(times) - 1))], 1),
        })  # fmt: skip
    return rows


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("meds_root", type=Path)
    p.add_argument("--sample", type=int, default=200, help="links read per encoding")
    a = p.parse_args()
    rows = profile(a.meds_root, a.sample)
    print(pl.DataFrame(rows))


if __name__ == "__main__":
    main()
