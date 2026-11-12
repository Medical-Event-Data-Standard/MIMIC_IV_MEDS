# Contributing

Pull requests are welcome. The ETL is configuration, not code, so most changes are edits to
`src/MIMIC_IV_MEDS/event_configs.yaml` or to an extension fragment. Each change should come with its
reasoning, recorded where it applies: as a comment in the YAML, and on this site if it is a
modelling decision users need to know about.

## Set up

```bash
git clone https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS
cd MIMIC_IV_MEDS
uv sync --group dev # add --group docs to build this site
uv run pre-commit install
```

## Tests

```bash
uv run pytest tests/
```

| Test                          | What it checks                                                                                                                                                                                                                                                                                                                        | Needs                               |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------- |
| `e2e_demo_test.py`            | `spec=MIMIC-IV` resolves by name, downloads the demo, and writes a valid cohort; every non-lab code is described; no description is multi-line                                                                                                                                                                                        | the open MIMIC-IV demo (downloaded) |
| `run_with_enrichment_test.py` | The `sumeds_enrich` post-stage, offline against a small synthetic ontology pack: enrichment is non-destructive and leaves the data unchanged, ICD codes get their ancestors and same-dataset parents, Gender gets its OMOP tag, `dataset.json` records it; a failed enrichment keeps the plain output and `suMEDS-enrich` finishes it | the open MIMIC-IV demo              |
| `sumeds_rules_test.py`        | `sumeds_rules.yaml` loads in suMEDS, is registered under the spec name, and matches the codes the spec's templates render                                                                                                                                                                                                             | nothing                             |
| `e2e_mne_synthetic_test.py`   | `spec=MIMIC-IV-MNE` over the demo plus synthetic MNE rows: exact `NOTE//` events, all at `dischtime`; extension columns and metadata                                                                                                                                                                                                  | the open MIMIC-IV demo              |
| `extension_specs_test.py`     | Generated specs are current, exactly base plus fragment, and registered                                                                                                                                                                                                                                                               | nothing                             |
| `code_metadata_test.py`       | Every event in every shipped spec declares exactly one description and has a code component                                                                                                                                                                                                                                           | nothing                             |

The demo is built once per test session, by the `demo_cohort` fixture in `tests/conftest.py`, with
`skip_stages=[sumeds_enrich]`, so it is the plain ETL output whatever the network allows.
`run_with_enrichment_test.py` reruns the ETL offline from the raw files that build downloaded. Set
`MEDS_DEMO_DOWNLOAD_DIR` to a durable directory to reuse the download between runs. Set
`MEDS_DEMO_OUTPUT_DIR` to keep the built demo cohort instead of a temporary one. CI does both,
caching the download for a week, and retries the tests only on network-shaped PhysioNet errors.

## Summary-statistics baselines

Two [meds_summary_stats](https://github.com/Medical-Event-Data-Standard/meds_summary_stats)
fingerprints of the ETL's output are committed under `tests/regression/`:

- **`demo_summary_stats.json`**, of the cohort built from the open MIMIC-IV demo.
- **`full_summary_stats.json`**, of the full MIMIC-IV 3.1 release. Because it is committed next to
    the ETL, its diff shows what an ETL change did to the whole credentialed release, and two ETL
    versions can be compared without access to the data.

A fingerprint holds subject, event and measurement counts, digests of the code vocabulary,
per-subject, value and time distributions, code-metadata coverage, the splits and the schema. It
names no code, and reports no cell of fewer than 20 subjects and no minimum or maximum, which is what
makes the full-release one safe to publish.

**The demo is checked on every pull request.** The Tests workflow compares the demo cohort it builds
with `demo_summary_stats.json`. The demo release is pinned, so the raw input never changes and any
difference is something this repository did. `tests/regression/policy.yaml` therefore treats the
cohort's content (counts, code-vocabulary digests, metadata coverage, splits) as an **error**, and
byte-level digests, which a dependency bump can move, as a warning. The job also uploads the new
statistics as the `mimic-demo-summary-stats` artifact.

**Regenerate the baselines in every pull request that changes the output.** CI cannot rebuild the
credentialed release, so the full baseline is your job.
[`scripts/summary_stats.sh`](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/blob/main/scripts/summary_stats.sh)
builds with this checkout, prints how the statistics changed against the committed baseline, and
overwrites it. `--compare` checks a build without writing anything:

```bash
# The demo: downloaded and built with this checkout
scripts/summary_stats.sh demo
# The full release, built from a copy on disk, or summarized from an existing build
scripts/summary_stats.sh full --input-dir $RAW_INPUT_DIR
scripts/summary_stats.sh full --meds-dir $MEDS_OUTPUT_DIR
# Compare only; exits 1 if the policy flags an error
scripts/summary_stats.sh full --meds-dir $MEDS_OUTPUT_DIR --compare
```

Both baselines are built with `skip_stages=[sumeds_enrich]`. They fingerprint what the ETL writes,
which does not depend on network access or on the suMEDS pack release, so summarize a build made the
same way.

meds_summary_stats is pinned to 0.0.4 (stats schema version 2) in three places that must move
together: the action and its `package-spec` in `.github/workflows/tests.yaml`, `STATS_SPEC` in
`scripts/summary_stats.sh`, and the committed baselines. A comparison across schema versions fails
with a precondition error, not a useful diff.

## Synthetic-data ETL

The Synthetic ETL workflow (`.github/workflows/synthetic-etl.yaml`) runs on Python 3.12 and 3.13. It
generates raw files straight from `event_configs.yaml` with
[MESSY_synth](https://pypi.org/project/MESSY-synth/) (0.1.0, the first release that reads
MEDS-Extract 0.8 configs) and runs the whole ETL over them with `do_download=false`, then checks that
`dataset.json`, `codes.parquet` and `subject_splits.parquet` were written. MESSY_synth fails the job
when an event block yields no events, so this catches expressions that only work on the dtypes of one
particular release. It complements the demo build, which needs a PhysioNet download and checks the
real output. The `sumeds_enrich` post-stage soft-fails without its ontology data, so it cannot fail
this job on its own.

To reproduce it locally:

```bash
uv pip install "MESSY_synth==0.1.0"
uv run --no-sync MESSY-synth ./src/MIMIC_IV_MEDS/event_configs.yaml -o raw_synthetic/ --explain
uv run --no-sync meds-extract-run spec=MIMIC-IV output_dir=./MIMIC_IV_meds_synth \
	input_dir=raw_synthetic do_download=false
```

## Generated extension specs

`src/MIMIC_IV_MEDS/event_configs_<name>.yaml` files are **generated**. Never edit them. After
changing `event_configs.yaml` or anything in `src/MIMIC_IV_MEDS/extensions/`, run:

```bash
uv run python scripts/build_extension_specs.py
```

Commit the regenerated file in the same change. `tests/extension_specs_test.py` (and
`--check` mode of the script) fails if you forget. See [Extensions](extensions/index.md) for how the
mechanism works and how to add a new extension.

## Pre-commit

```bash
uv run pre-commit run --all-files
```

This runs ruff, docformatter, prettier (YAML, including the generated specs), shellcheck, codespell,
and mdformat with the `mdformat-mkdocs` plugin. mdformat therefore also formats `docs/`. CI runs the
same hooks on changed files.

## Documentation

This site is built with [Zensical](https://zensical.org/), the successor to Material for MkDocs,
from `zensical.toml` and `docs/`.

```bash
uv sync --group docs
uv run --group docs zensical serve          # live preview at http://localhost:8000
uv run --group docs zensical build --strict # what CI and Read the Docs run; warnings are errors
```

The build writes the site to `site/` and a cache to `.cache/`; both are git-ignored. In strict mode a
broken link or a missing anchor fails the build.

- **Navigation is generated** from the `docs/` tree by Zensical's built-in support for
    [mkdocs-awesome-nav](https://lukasgeiter.github.io/mkdocs-awesome-nav/) `.nav.yml` files (enabled
    by `[project.plugins.awesome-nav]` in `zensical.toml`; the MkDocs plugin itself is not installed).
    `docs/.nav.yml` orders the top level. `docs/extensions/.nav.yml` puts the overview first and
    globs every other page. A new page in `docs/extensions/` appears automatically, titled by its
    first `#` heading. Do not add a `nav` key to `zensical.toml`.
- **A new top-level page must be listed in `docs/.nav.yml`.** Otherwise it is built but left out of
    the navigation, and the build does not warn about it.
- **Extension pages** follow the template of [MIMIC-IV-MNE](extensions/mne.md): summary, source,
    how to run, tables → events, ETL decisions, caveats and limitations, not extracted, validation.
- **Never put row-level data from credentialed datasets in the docs**, even as an example. Use
    aggregate figures only. The public demo is fine to inspect.
- **Read the Docs** builds from `.readthedocs.yaml`, with custom build jobs that install the `docs`
    dependency group with uv, run `zensical build --strict` and copy `site/` to RTD's output
    directory.
