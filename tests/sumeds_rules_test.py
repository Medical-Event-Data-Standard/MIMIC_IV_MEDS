"""The packaged suMEDS rules load in suMEDS and match the codes this ETL emits.

suMEDS validates `sumeds_rules.yaml` itself (`sumeds.load_rules`) and resolves codes with the rule
objects its enrichment uses, so this test only states what MIMIC-IV's codes must resolve to. Example
codes are rendered from the code templates in `event_configs.yaml` with synthetic values, so a change to
a code layout that the rules no longer match fails here rather than silently dropping the enrichment.
"""

import re
from importlib.resources import files

import pytest
import yaml
from sumeds import load_rules, registered_rules

RULES_FP = files("MIMIC_IV_MEDS").joinpath("sumeds_rules.yaml")
CONFIG = yaml.safe_load(files("MIMIC_IV_MEDS").joinpath("event_configs.yaml").read_text())

#: Top-level keys of the MESSY file that are not raw tables.
RESERVED_KEYS = {"etl", "sources", "_defaults"}

#: Synthetic component values used to render every code template of the config.
DEFAULT_VALUE = "X1"
VALUES = {"gender": "F", "icd_version": "10", "icd_code": "I5023", "itemid": "50912", "valueuom": "mg/dL"}


def _resolved(code):
    """The ontology rules that resolve ``code``, with the vocabulary code each yields."""
    return {rule.id: value for rule in load_rules(RULES_FP).ontology if (value := rule.resolve(code))}


def _events():
    for table, events in CONFIG.items():
        if table in RESERVED_KEYS:
            continue
        constants = events.get("_table", {}).get("cols", {})
        for name, event in events.items():
            if name != "_table":
                yield f"{table}/{name}", str(event["code"]).strip(), constants


def _render(template, constants, values):
    """Render a MESSY code expression (`f"..."` or a bare `$column`) with synthetic values."""
    template = template.strip().strip("'")
    if template.startswith("$"):
        column = template[1:].split()[0]
        return constants[column].strip("'") if column in constants else values.get(column, DEFAULT_VALUE)
    assert template.startswith('f"') and template.endswith('"'), template
    return re.sub(
        r"\{\$(\w+)(?:\s*\?\?\s*'[^']*')?\}",
        lambda m: values.get(m.group(1), DEFAULT_VALUE),
        template[2:-1],
    )


def _code(event_name, **values):
    template, constants = next((t, c) for n, t, c in _events() if n == event_name)
    return _render(template, constants, values)


def test_rules_load_in_sumeds():
    """The rules load in suMEDS, which rejects any malformed rule."""
    rules = load_rules(RULES_FP)
    assert rules.ontology and rules.hierarchy


def test_rules_are_registered_under_the_spec_name():
    """`suMEDS-extract spec=MIMIC-IV` and `--rules MIMIC-IV` find the rules by this entry point."""
    assert registered_rules()["MIMIC-IV"] == "MIMIC_IV_MEDS:sumeds_rules.yaml"
    assert load_rules("MIMIC-IV") == load_rules(RULES_FP)


def test_rules_target_the_configured_events():
    """Every rendered code of the config matches only the rules meant for its event."""
    expected = {
        "hosp/patients/gender": {"mimic-gender"},
        "hosp/diagnoses_icd/diagnosis": {"mimic-icd10cm-diagnoses"},
    }
    seen = set()
    for name, template, constants in _events():
        code = _render(template, constants, VALUES)
        matched = set(_resolved(code))
        assert matched == expected.get(name, set()), (name, code, matched)
        seen.add(name)
    assert set(expected) <= seen, "an event the rules target is no longer in the config"


@pytest.mark.parametrize(
    "event,version,icd_code,rule_id,vocabulary_code",
    [
        ("hosp/diagnoses_icd/diagnosis", "10", "I5023", "mimic-icd10cm-diagnoses", "I50.23"),
        ("hosp/diagnoses_icd/diagnosis", "10", "I10", "mimic-icd10cm-diagnoses", "I10"),
        ("hosp/diagnoses_icd/diagnosis", "10", "C7A010", "mimic-icd10cm-diagnoses", "C7A.010"),
        ("hosp/diagnoses_icd/diagnosis", "9", "4019", "mimic-icd9cm-diagnoses", "401.9"),
        ("hosp/diagnoses_icd/diagnosis", "9", "25000", "mimic-icd9cm-diagnoses", "250.00"),
        ("hosp/diagnoses_icd/diagnosis", "9", "401", "mimic-icd9cm-diagnoses", "401"),
        ("hosp/diagnoses_icd/diagnosis", "9", "V1052", "mimic-icd9cm-diagnoses", "V10.52"),
        ("hosp/diagnoses_icd/diagnosis", "9", "E8889", "mimic-icd9cm-diagnoses", "E888.9"),
        ("hosp/diagnoses_icd/diagnosis", "9", "E888", "mimic-icd9cm-diagnoses", "E888"),
        ("hosp/procedures_icd/procedure", "10", "0DTJ4ZZ", "mimic-icd10pcs-procedures", "0DTJ4ZZ"),
        ("hosp/procedures_icd/procedure", "9", "3961", "mimic-icd9-procedures", "39.61"),
        ("hosp/procedures_icd/procedure", "9", "961", "mimic-icd9-procedures", "96.1"),
        ("hosp/procedures_icd/procedure", "9", "99", "mimic-icd9-procedures", "99"),
    ],
)
def test_icd_codes_normalize_to_the_pack_format(event, version, icd_code, rule_id, vocabulary_code):
    code = _code(event, icd_version=version, icd_code=icd_code)
    assert _resolved(code) == {rule_id: vocabulary_code}, code


@pytest.mark.parametrize("gender,expected", [("F", "F"), ("M", "M"), ("UNK", None)])
def test_gender_is_tagged_with_the_omop_gender_code(gender, expected):
    rule = next(rule for rule in load_rules(RULES_FP).ontology if rule.id == "mimic-gender")
    assert rule.vocabulary == "Gender" and rule.validate == "none"
    assert rule.resolve(_code("hosp/patients/gender", gender=gender)) == expected


def test_hierarchy_covers_every_icd_rule():
    rules = load_rules(RULES_FP)
    (rule,) = rules.hierarchy
    assert rule.mode == "ontology"
    assert set(rule.rules) == {r.id for r in rules.ontology if r.id.startswith("mimic-icd")}
