# Building released versions that no longer install

Seven of this ETL's ten released versions do not install into a working pipeline today. In
every case the released **code is fine** — only the upstream version specifier is wrong, so a
resolver run now picks a MEDS-Transforms or MEDS-Extract release years newer than the one the
version was written against.

This directory records the upstream that actually works for each release, so any published
version can still be rebuilt, run, and audited.

```bash
compat/build_version.sh 0.1.2 /tmp/mimic-0.1.2
PATH="/tmp/mimic-0.1.2/.venv/bin:$PATH" \
  /tmp/mimic-0.1.2/.venv/bin/MEDS_extract-MIMIC_IV root_output_dir=$OUT do_demo=True
```

## What was wrong, per version

| version | declared | resolves to | failure | works with |
|---|---|---|---|---|
| 0.0.1 | `meds-transforms` | 0.6.7 | `ModuleNotFoundError: MEDS_transforms.extract` | `==0.0.9` |
| 0.0.2 | `meds-transforms` | 0.6.7 | same | `==0.0.9` |
| 0.0.3 | `meds-transforms>=0.1` | 0.6.7 | same | `==0.1` |
| 0.0.4 | `meds-transforms~=0.2` | 0.6.7 | same | `==0.2.1` |
| 0.0.5 | `meds-transforms~=0.2.1` | 0.2.4 | ✅ installs and runs | *(pinned for exactness)* |
| 0.0.6 | `meds-transforms~=0.2.1` | 0.2.4 | ✅ | *(pinned for exactness)* |
| 0.0.7 | `meds-transforms~=0.2.1` | 0.2.4 | ✅ | *(pinned for exactness)* |
| 0.1.0 | `meds-extract~=0.5` | 0.6.2 | `Failed to parse expression "['HCPCS', 'col(short_description)']"` | `meds-extract==0.5.0` |
| 0.1.1 | `meds-extract~=0.5` | 0.6.2 | same | `meds-extract==0.5.0` |
| 0.1.2 | `meds-extract~=0.5` | 0.6.2 | same | `meds-extract==0.5.0` |

Two distinct breakages:

- **0.0.1–0.0.4** — extraction moved out of MEDS-Transforms into MEDS-Extract, so
  `MEDS_transforms.extract` disappeared. Any unbounded `meds-transforms` requirement walks
  straight into it.
- **0.1.0–0.1.2** — MEDS-Extract 0.6 routes code specifications through dftly's expression
  parser, which rejects the YAML list form (`[HCPCS, col(short_description)]`) that these
  configs use. Verified that 0.6.1 fails identically to 0.6.2, so the boundary is 0.5 → 0.6,
  not a 0.6.2 regression.

`~=0.2.1` (in 0.0.5–0.0.7) is the only specifier in this ETL's history that means what its
author intended: `>=0.2.1,<0.3`. **`~=0.2` and `~=0.5` both admit the next minor**, and both
MEDS-Transforms and MEDS-Extract make breaking changes across minors while pre-1.0.

## Why this is a branch and not a fix on `main`

The right *forward* fix is to cap the upstream at the next minor, and `main` has already moved
far past these releases — it is mid-migration to MEDS-Extract 0.7, where the ETL is pure
config. Retro-fitting `main` would not help anyone trying to rebuild 0.0.3.

Fixing this properly for consumers would mean publishing patch releases of each line
(`0.1.3` with `meds-extract>=0.5,<0.6`, and so on) or yanking the versions that cannot be
installed correctly. That is a maintainer decision. Until then, this directory is the
executable record of how to get each one running, and it costs `main` nothing.

## How these were verified

Each version was installed into an isolated venv from PyPI with its constraints file, then run
against a **single cached copy** of the PhysioNet `mimic-iv-demo/2.2` release with downloading
disabled — byte-identical input for every version, so any difference in output is attributable
to the ETL rather than to a re-download.

All ten produce a valid MEDS cohort. Their summary-stats fingerprints are near-identical: the
only movement in the entire published history is a deliberate expansion at 0.0.7 → 0.1.0
(events 83,419 → 96,949). Every version also stamps `dataset_version: "3.1:<etl>"` onto a
cohort built from the **2.2** demo release — a mislabelling fixed only on the 0.7 branch.

See [issue #66](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/issues/66).
