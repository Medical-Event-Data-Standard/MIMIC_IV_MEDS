# Extensions

An *extension* adds a companion PhysioNet dataset to the base MIMIC-IV ETL. Each one is its own
registered spec, such as `spec=MIMIC-IV-MNE`. That spec is the complete [base ETL](../base-etl.md)
plus the extension's tables. `spec=MIMIC-IV` and its output are never changed by an extension.

## Overview

| Extension                    | Source                                                                                              | Adds                                                                                                                                                                                                      | Timing rule                                | Code prefix | Access                                                                                  |
| ---------------------------- | --------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ | ----------- | --------------------------------------------------------------------------------------- |
| [`MIMIC-IV-MNE`](mne.md)     | [MIMIC-IV-Ext MIMIC-Note-Extract 1.0](https://physionet.org/content/mimic-iv-ext-note-extract/1.0/) | Structured discharge-summary content: past medical history, home and discharge medications, allergies, physical exam and vitals, chief complaint, discharge diagnoses, procedures, condition at discharge | Every event at `hosp/admissions.dischtime` | `NOTE//`    | Credentialed; own DUA; no demo                                                          |
| MIMIC-IV-ED *(in progress)*  | [MIMIC-IV-ED 2.2](https://physionet.org/content/mimic-iv-ed/2.2/)                                   | ED stays, triage, ED vital signs, ED diagnoses, medication reconciliation and dispensing ([#74](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/issues/74))                                  | TBD                                        | TBD         | Credentialed; own DUA; open [demo](https://physionet.org/content/mimic-iv-ed-demo/2.2/) |
| MIMIC-CXR-DB *(in progress)* | [MIMIC-CXR-Ext-MIMIC-CXR-DB 1.0](https://physionet.org/content/mimic-cxr-ext-mimic-cxr-db/1.0/)     | Chest radiographs linked to hospital admissions, with harmonized clinical context                                                                                                                         | TBD                                        | TBD         | Credentialed; own DUA                                                                   |

Each extension has its own page in this section, following the same template: summary, source, how
to run, tables to events, ETL decisions, caveats, what is not extracted, and validation. Sources and
citations for all of them are collected in [Sources and citations](../sources.md).

## Why extensions are separate specs

Two constraints rule out putting the extra tables into `event_configs.yaml`:

1. **MEDS-Extract has no optional tables.** It runs one spec file and fails on any configured table
    whose raw file is missing. It has no include, optional-table or conditional-block mechanism
    (neither 0.7 nor 0.8.1 has one). An extension's tables in the base spec would make that
    dataset mandatory for every user.
2. **Each companion dataset needs its own data use agreement.** MIMIC-Note-Extract, MIMIC-IV-ED and
    MIMIC-CXR-DB are separate PhysioNet projects. Approval for MIMIC-IV does not grant access to them,
    and many users do not have it.

So each extension is a separate spec that users opt into by name. Keeping them separate also keeps
`spec=MIMIC-IV` output comparable across releases (for example with existing MEDS-DEV results),
because adding events changes cohorts, event counts and task labels.

## How it works: fragment, generator, freshness test

A hand-maintained copy of the base for each extension would drift. Instead, an extension is written
as a *fragment*, and a script generates the spec:

```text
src/MIMIC_IV_MEDS/
├── event_configs.yaml           # the base spec (MIMIC-IV) — edit this
├── extensions/
│   └── mne.yaml                 # the MNE fragment — edit this
└── event_configs_mne.yaml       # GENERATED: base + fragment (MIMIC-IV-MNE) — never edit
```

**The fragment** (`src/MIMIC_IV_MEDS/extensions/<name>.yaml`) holds:

- a header comment that explains the extension's rules;
- `etl.dataset_name`, which becomes the spec name. It is the only `etl:` key the generator reads;
    everything else in the generated `etl:` block comes from the base (see below);
- the extension's table blocks (these may join base tables such as `hosp/admissions`, but may not
    redefine one);
- a `sources:` block whose buckets are *appended* to the base buckets of the same name. A base bucket
    the fragment does not name is dropped from the generated spec. MNE names only `dataset`, so
    `demo` is dropped, because MNE has no demo data and a demo run would fail on its missing tables.
    `common` (the concept maps) is always kept.

**The generator**,
[`scripts/build_extension_specs.py`](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/blob/main/scripts/build_extension_specs.py),
writes `src/MIMIC_IV_MEDS/event_configs_<name>.yaml` for every fragment. The output contains every
base block except `sources:` verbatim (comments included), with `etl.dataset_name` replaced, then
the fragment's header comment and tables verbatim, then the merged `sources:` block. It refuses a
fragment that has no `etl.dataset_name` or no `sources:` block, redefines a base table, or names a
bucket the base does not have. The merged `sources:` is written with a YAML style that matches the
repo's prettier hook, so pre-commit does not rewrite it. The generated file is committed, because
the entry point needs a file inside the package.

Because the base `etl:` block is copied whole, every extension spec inherits its run settings: the
extension-column roles written to `dataset.json` (`raw_source_id_columns`, `code_modifier_columns`)
and the `sumeds_enrich` post-stage with the `MIMIC-IV` suMEDS rules (see
[Base ETL → Enrichment with suMEDS](../base-etl.md#enrichment-with-sumeds)). A fragment cannot add to
them, so an identifier column that only the extension emits is listed under
`other_extension_columns`.

```bash
uv run python scripts/build_extension_specs.py         # rewrite the generated specs
uv run python scripts/build_extension_specs.py --check # exit 1 if any is stale
```

**The freshness test**,
[`tests/extension_specs_test.py`](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/blob/main/tests/extension_specs_test.py),
needs no data. It fails if a generated spec is out of date (an edit to the base or a fragment
without re-running the script). It also fails if the spec is not exactly base plus fragment, or if
the spec name is not registered as an entry point.

**Registration.** Each generated file is registered in `pyproject.toml`, under
`[project.entry-points."MEDS_extract.pipelines"]`, so `spec=<name>` works anywhere:

```toml
[project.entry-points."MEDS_extract.pipelines"]
MIMIC-IV = "MIMIC_IV_MEDS:event_configs.yaml"
MIMIC-IV-MNE = "MIMIC_IV_MEDS:event_configs_mne.yaml"
```

!!! note "Combining extensions"

    The generator composes the base with **one** fragment at a time. Running two extensions together
    (say ED and MNE) would need another generated spec per combination. The issue
    [#81 discussion](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/issues/81)
    suggests the real fix is upstream: letting MEDS-Extract skip tables whose source is not present,
    so that one spec uses whichever datasets a user has.

## Adding a new extension

1. **Write the fragment** at `src/MIMIC_IV_MEDS/extensions/<name>.yaml`. Set
    `etl.dataset_name: MIMIC-IV-<NAME>`, add the table blocks, and add a `sources:` block that names
    the buckets it extends: `dataset`, plus `demo` if the dataset has an open demo. Use
    `extensions/mne.yaml` as the model, including its header comment.
2. **Generate the spec:** `uv run python scripts/build_extension_specs.py`.
3. **Register it** in `pyproject.toml` under `[project.entry-points."MEDS_extract.pipelines"]` as
    `MIMIC-IV-<NAME> = "MIMIC_IV_MEDS:event_configs_<name>.yaml"`, then reinstall (`uv sync`) so the
    entry point is visible.
4. **Add it to the tests:** add the generated file to `SPECS` in `tests/code_metadata_test.py`, and
    add an end-to-end test. Use the open demo if there is one; otherwise use synthetic rows over the
    MIMIC-IV demo, as `tests/e2e_mne_synthetic_test.py` does.
5. **Document it:** add `docs/extensions/<name>.md` with the eight-section template used by
    [MIMIC-IV-MNE](mne.md), add a row to the table above, and add the dataset to
    [Sources and citations](../sources.md). The page appears in the site navigation automatically.
    No `mkdocs.yml` edit is needed.
6. Run `uv run pytest tests/` and `uv run pre-commit run --all-files`.

Whenever `event_configs.yaml` changes later, re-run the generator in the same change. The freshness
test catches it if you forget.
