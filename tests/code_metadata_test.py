"""Every event in the MESSY config describes the codes it emits.

This reads `event_configs.yaml` only and needs no data, so it runs in milliseconds. Whether
the descriptions actually land in `metadata/codes.parquet` is checked on the built demo cohort
by `e2e_demo_test.py`.
"""

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


def test_every_code_has_a_component():
    """A literal code cannot carry metadata: MEDS-Extract attaches it only through components.

    Literal codes are rendered from a constant `_table.cols` column instead (see
    `ed_registration_code` in the config).
    """
    literal = [name for name, event in _events() if "$" not in str(event["code"])]
    assert not literal, f"literal code expressions (no $column) cannot be described: {literal}"


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
