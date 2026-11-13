"""Compose the base spec with any combination of module fragments into one MESSY spec.

Unlike `scripts/build_extension_specs.py`, which writes one registered spec per extension and keeps
the YAML comments, this composes at build time, for whichever modules were asked for, so every
combination works without a registered spec per subset. The composed spec:

- has every event table of the base, plus every table of each module's fragment (ED reuses the
  registered `extensions/ed.yaml`; the others are `multimodal/fragments/<module>.yaml`);
- has no `sources:` block: the build stages the raw input itself and runs MEDS-Extract with
  `do_download=false`;
- describes the combination in its `etl:` block, which MEDS-Extract writes to
  `metadata/dataset.json`:
  - `dataset_name`: `MIMIC-IV` plus each module's label, e.g. `MIMIC-IV+ED+ECG`;
  - `raw_dataset_version`: every release used, as `<slug>/<version>`, comma-separated (MEDS-Extract
    appends `:<package version>`);
  - `license`: the PhysioNet Credentialed Health Data License as soon as any release used is
    credentialed, else ODbL;
  - `additional_value_modality_columns`: the link column of each object module;
  - `raw_source_id_columns`: the base's plus each module's raw-release keys.
"""

from __future__ import annotations

from importlib.resources import files
from pathlib import Path

import yaml

from .modules import MODULES, ODBL, PHYSIONET_CREDENTIALED, FoundRelease

PKG = files("MIMIC_IV_MEDS")
RESERVED = ("etl", "sources")


def base_spec() -> dict:
    return yaml.safe_load(PKG.joinpath("event_configs.yaml").read_text())


def fragment(module: str) -> dict:
    """A module's event tables (its `etl:` and `sources:` blocks, if any, are dropped)."""
    if module == "ed":
        text = PKG.joinpath("extensions", "ed.yaml").read_text()
    else:
        text = PKG.joinpath("multimodal", "fragments", f"{module}.yaml").read_text()
    return {k: v for k, v in yaml.safe_load(text).items() if k not in RESERVED}


def compose(core: FoundRelease, found: list[FoundRelease], skip_tables: set[str] = frozenset()) -> dict:
    """The spec for `core` plus the modules in `found` (in that order), without `skip_tables`.

    `skip_tables` are optional fragment tables whose raw input the build did not find (e.g. the echo
    measurements); MEDS-Extract fails on a configured table without input, so they are left out.
    """
    base = base_spec()
    tables = {k: v for k, v in base.items() if k not in RESERVED}
    # Parts not asked for contribute nothing, whether or not staging reported their tables.
    skip_tables = set(skip_tables) | {
        t for f in found for p in f.module.parts if not f.wants(p.name) for t in p.tables
    }
    for f in found:
        frag = {k: v for k, v in fragment(f.module.name).items() if k not in skip_tables}
        if clash := sorted(set(frag) & set(tables)):
            raise ValueError(f"module {f.module.name} redefines tables {clash}")
        tables.update(frag)

    releases = [core, *found]
    etl = dict(base["etl"])
    etl["dataset_name"] = "+".join([core.module.label, *(f.label for f in found)])
    etl["raw_dataset_version"] = ",".join(r.tag for r in releases)
    etl["license"] = PHYSIONET_CREDENTIALED if any(r.release.credentialed for r in releases) else ODBL
    links = link_columns(found, skip_tables)
    if links:
        etl["additional_value_modality_columns"] = links
    raw_ids = list(etl.get("raw_source_id_columns", []))
    for f in found:
        raw_ids += [c for c in module_columns(f, skip_tables)[1] if c not in raw_ids]
    etl["raw_source_id_columns"] = raw_ids
    return {"etl": etl, **tables}


def active_parts(f: FoundRelease, skip_tables: set[str] = frozenset()) -> tuple[str, ...]:
    """The parts of `f` that were asked for and built (a part whose tables were all skipped was not)."""
    return tuple(
        p.name
        for p in f.module.parts
        if f.wants(p.name) and not (p.tables and set(p.tables) <= set(skip_tables))
    )


def module_columns(f: FoundRelease, skip_tables: set[str] = frozenset()) -> tuple[list[str], list[str]]:
    """`(link columns, raw source id columns)` that `f` puts in the data, given what was built.

    MEDS-Extract rejects a `dataset.json` category naming a column its output lacks, so a part that was
    not built must not contribute its columns.
    """
    links = [f.module.link_column] if f.module.link_column else []
    raw = list(f.module.raw_source_id_columns)
    active = set(active_parts(f, skip_tables))
    for p in f.module.parts:
        if p.name in active:
            links += [p.link_column] if p.link_column and p.link_column not in links else []
            raw += [c for c in p.raw_source_id_columns if c not in raw]
    return links, raw


def link_columns(found: list[FoundRelease], skip_tables: set[str] = frozenset()) -> list[str]:
    return [c for f in found for c in module_columns(f, skip_tables)[0]]


def common_sources() -> list[dict]:
    """The base spec's `sources.common` entries (the public concept maps its tables join)."""
    return base_spec().get("sources", {}).get("common", [])


def write_spec(spec: dict, fp: Path) -> Path:
    fp.parent.mkdir(parents=True, exist_ok=True)
    header = (
        "# GENERATED by MIMIC_IV_MEDS.multimodal for this build: the base event_configs.yaml plus the\n"
        "# fragments of the modules asked for. Regenerated on every build; do not edit.\n"
    )
    fp.write_text(header + yaml.safe_dump(spec, sort_keys=False, allow_unicode=True, width=10**9))
    return fp


__all__ = ["MODULES", "common_sources", "compose", "write_spec"]
