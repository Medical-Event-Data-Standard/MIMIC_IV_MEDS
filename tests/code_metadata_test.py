"""Every event in the MESSY config describes the codes it emits.

This reads `event_configs.yaml` only and needs no data, so it runs in milliseconds. Whether
the descriptions actually land in `metadata/codes.parquet` is checked on the built demo cohort
by `e2e_demo_test.py`.
"""

import re
from importlib.resources import files

import yaml

#: Top-level keys of the MESSY file that are not raw tables.
RESERVED_KEYS = {"etl", "sources", "_defaults"}


def _events():
    cfg = yaml.safe_load(files("MIMIC_IV_MEDS").joinpath("event_configs.yaml").read_text())
    for table, events in cfg.items():
        if table in RESERVED_KEYS:
            continue
        for name, event in events.items():
            if name != "_table":
                yield f"{table}/{name}", event


def test_literal_codes_are_plain_literals():
    """The literal codes are written as literals, not read from constant `_table.cols` columns.

    MEDS-Extract >= 0.8.1 attaches `_self` metadata to literal codes (mmcdermott/MEDS_extract#166),
    so a literal needs no column to be described.
    """
    literal = {
        str(event["code"]).strip().strip("'") for _, event in _events() if "$" not in str(event["code"])
    }
    assert literal == {
        "ED_REGISTRATION",
        "ED_OUT",
        "MEDS_BIRTH",
        "MEDS_DEATH",
        "SUBJECT_WEIGHT_AT_INFUSION",
    }
    from_a_column = [name for name, event in _events() if re.fullmatch(r"\$\w+", str(event["code"]).strip())]
    assert not from_a_column, f"codes read from a bare column: {from_a_column}"


def test_every_event_declares_one_description():
    """Exactly one `_metadata` block per event emits `description`.

    None leaves the event's codes undescribed. Two would make MEDS-Extract join both strings with a newline
    into one description for every code both blocks match.
    """
    counts = {
        name: sum("description" in block for block in event.get("_metadata", {}).values())
        for name, event in _events()
    }
    assert counts, "no events found in event_configs.yaml"
    wrong = {name: n for name, n in counts.items() if n != 1}
    assert not wrong, f"events with no or several description sources: {wrong}"
