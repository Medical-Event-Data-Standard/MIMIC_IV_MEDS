"""Every event in each shipped MESSY spec describes the codes it emits.

This reads the spec files only and needs no data, so it runs in milliseconds. Whether the
descriptions actually land in `metadata/codes.parquet` is checked on built cohorts by
`e2e_demo_test.py` (the base spec) and the `e2e_*_synthetic_test.py` files (the extensions).
"""

import re
from importlib.resources import files

import pytest
import yaml

#: Every registered spec. The extension specs are generated from the base plus a fragment, so
#: checking them checks the fragments' events.
SPECS = [
    "event_configs.yaml",
    *sorted(
        f"event_configs_{p.name.removesuffix('.yaml')}.yaml"
        for p in files("MIMIC_IV_MEDS").joinpath("extensions").iterdir()
        if p.name.endswith(".yaml")
    ),
]

#: Top-level keys of the MESSY file that are not raw tables.
RESERVED_KEYS = {"etl", "sources", "_defaults"}


def _events(spec: str):
    cfg = yaml.safe_load(files("MIMIC_IV_MEDS").joinpath(spec).read_text())
    for table, events in cfg.items():
        if table in RESERVED_KEYS:
            continue
        for name, event in events.items():
            # `_table` and `_defaults` are table settings; MEDS-Extract reserves every `_` name.
            if not name.startswith("_"):
                yield f"{table}/{name}", event


#: The base spec's literal codes, which every extension spec inherits.
LITERAL_CODES = {
    "ED_REGISTRATION",
    "ED_OUT",
    "MEDS_BIRTH",
    "MEDS_DEATH",
    "SUBJECT_WEIGHT_AT_INFUSION",
}

#: The literal codes an extension adds to the base's. MNE's two medication lists are one code each
#: (the drug is free text in `text_value`). Every other extension code reads a column of its row.
EXTENSION_LITERAL_CODES = {
    "event_configs_mne.yaml": {"NOTE//MEDICATION//ON_ADMISSION", "NOTE//MEDICATION//AT_DISCHARGE"},
}


@pytest.mark.parametrize("spec", SPECS)
def test_literal_codes_are_plain_literals(spec):
    """The literal codes are written as literals, not read from constant `_table.cols` columns.

    MEDS-Extract >= 0.8.1 attaches `_self` metadata to literal codes (mmcdermott/MEDS_extract#166),
    so a literal needs no column to be described.
    """
    literal = {
        str(event["code"]).strip().strip("'") for _, event in _events(spec) if "$" not in str(event["code"])
    }
    assert literal == LITERAL_CODES | EXTENSION_LITERAL_CODES.get(spec, set())
    from_a_column = [
        name for name, event in _events(spec) if re.fullmatch(r"\$\w+", str(event["code"]).strip())
    ]
    assert not from_a_column, f"codes read from a bare column: {from_a_column}"


@pytest.mark.parametrize("spec", SPECS)
def test_every_event_declares_one_description(spec):
    """Exactly one `_metadata` block per event emits `description`.

    None leaves the event's codes undescribed. Two would make MEDS-Extract join both strings with a newline
    into one description for every code both blocks match.
    """
    counts = {
        name: sum("description" in block for block in event.get("_metadata", {}).values())
        for name, event in _events(spec)
    }
    assert counts, f"no events found in {spec}"
    wrong = {name: n for name, n in counts.items() if n != 1}
    assert not wrong, f"events with no or several description sources: {wrong}"
