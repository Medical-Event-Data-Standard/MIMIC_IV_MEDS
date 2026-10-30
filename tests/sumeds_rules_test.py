"""The packaged suMEDS rules parse and match the codes this ETL emits.

`sumeds_rules.yaml` is read by suMEDS (`sumeds-rules/0.1`), which validates it strictly at run
time. This test needs no suMEDS install: it repeats the format checks that matter (keys, ids,
regular expressions and their named groups) and renders example codes from the code templates
in `event_configs.yaml` with synthetic values, so a change to a code layout that the rules no
longer match fails here rather than silently dropping the enrichment.
"""

import re
from importlib.resources import files

import pytest
import yaml

RULES = yaml.safe_load(files("MIMIC_IV_MEDS").joinpath("sumeds_rules.yaml").read_text())
CONFIG = yaml.safe_load(files("MIMIC_IV_MEDS").joinpath("event_configs.yaml").read_text())

#: Allowed keys per rule kind, as in suMEDS (`sumeds.ontology`).
ONTOLOGY_KEYS = {"id", "pattern", "vocabulary", "normalize", "validate"}
CROSSWALK_KEYS = ONTOLOGY_KEYS | {"table", "key_column", "code_column", "where"}
HIERARCHY_KEYS = {
    "id",
    "pattern",
    "group",
    "mode",
    "separator",
    "ignore",
    "nearest_only",
    "from_ontology",
    "rules",
}
VALIDATE_MODES = {"pack", "athena", "none"}
#: Top-level keys of the MESSY file that are not raw tables.
RESERVED_KEYS = {"etl", "sources", "_defaults"}

#: Synthetic component values used to render every code template of the config.
DEFAULT_VALUE = "X1"
VALUES = {"gender": "F", "icd_version": "10", "icd_code": "I5023", "itemid": "50912", "valueuom": "mg/dL"}


def _vocabulary_rules():
    return [*RULES.get("ontology", []), *RULES.get("crosswalk", [])]


def _all_rules():
    return [*_vocabulary_rules(), *RULES.get("hierarchy", [])]


def _ontology(rule_id):
    (rule,) = [rule for rule in RULES["ontology"] if rule["id"] == rule_id]
    return rule


def _resolve(rule, code):
    """The vocabulary code an ontology rule yields for ``code`` (suMEDS semantics), or None."""
    match = re.search(rule["pattern"], code)
    if match is None or match.group("code") is None:
        return None
    value = match.group("code")
    for step in rule.get("normalize", []):
        value = re.sub(step["pattern"], step["replace"], value)
    return value


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


def test_rules_file_format():
    assert RULES["schema"] == "sumeds-rules/0.1"
    assert set(RULES) <= {"schema", "ontology", "crosswalk", "hierarchy"}
    ids = [rule["id"] for rule in _all_rules()]
    assert all(isinstance(i, str) and i.strip() for i in ids)
    assert len(ids) == len(set(ids)), "rule ids must be unique"
    for rule in RULES.get("ontology", []):
        assert set(rule) <= ONTOLOGY_KEYS, rule["id"]
        assert "code" in re.compile(rule["pattern"]).groupindex, rule["id"]
    for rule in RULES.get("crosswalk", []):
        assert set(rule) <= CROSSWALK_KEYS, rule["id"]
        assert "key" in re.compile(rule["pattern"]).groupindex, rule["id"]
        assert re.fullmatch(r"[A-Za-z0-9][\w.-]*/[A-Za-z0-9][\w.-]*", rule["table"]), rule["id"]
    for rule in _vocabulary_rules():
        assert rule.get("validate", "pack") in VALIDATE_MODES, rule["id"]
        assert rule["vocabulary"].strip() and "/" not in rule["vocabulary"], rule["id"]
        for step in rule.get("normalize", []):
            assert set(step) == {"pattern", "replace"}, rule["id"]
            pattern = re.compile(step["pattern"])
            for name, number in re.findall(r"\\(?:g<([^>]*)>|(\d{1,2}))", step["replace"]):
                ref = name or number
                assert int(ref) <= pattern.groups if ref.isdigit() else ref in pattern.groupindex, rule["id"]
    vocabulary_ids = {rule["id"] for rule in _vocabulary_rules()}
    for rule in RULES.get("hierarchy", []):
        assert set(rule) <= HIERARCHY_KEYS, rule["id"]
        if rule.get("from_ontology"):
            assert set(rule) <= {"id", "from_ontology", "nearest_only", "rules"}, rule["id"]
            assert set(rule.get("rules", [])) <= vocabulary_ids, rule["id"]
        else:
            assert rule["group"] in re.compile(rule["pattern"]).groupindex, rule["id"]
            assert ("separator" in rule) != (rule.get("mode") == "prefix"), rule["id"]


def test_rules_target_the_configured_events():
    """Every rendered code of the config matches only the rules meant for its event."""
    expected = {
        "hosp/patients/gender": {"mimic-gender"},
        "hosp/diagnoses_icd/diagnosis": {"mimic-icd10cm-diagnoses"},
    }
    seen = set()
    for name, template, constants in _events():
        code = _render(template, constants, VALUES)
        matched = {rule["id"] for rule in RULES["ontology"] if _resolve(rule, code)}
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
    resolved = {rule["id"]: _resolve(rule, code) for rule in RULES["ontology"]}
    assert {k: v for k, v in resolved.items() if v} == {rule_id: vocabulary_code}, code


@pytest.mark.parametrize("gender,expected", [("F", "F"), ("M", "M"), ("UNK", None)])
def test_gender_is_tagged_with_the_omop_gender_code(gender, expected):
    rule = _ontology("mimic-gender")
    assert rule["vocabulary"] == "Gender" and rule["validate"] == "none"
    assert _resolve(rule, _code("hosp/patients/gender", gender=gender)) == expected


def test_hierarchy_covers_every_icd_rule():
    (rule,) = RULES["hierarchy"]
    assert rule["from_ontology"] is True
    assert set(rule["rules"]) == {r["id"] for r in RULES["ontology"] if r["id"].startswith("mimic-icd")}
