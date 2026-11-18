"""The MIMIC-IV MEDS ETL — a configuration package, not a library.

The ETL is ``MIMIC_IV_MEDS/event_configs.yaml``, a MESSY (MEDS-Extract Specification Syntax YAML)
document declaring where the raw MIMIC-IV data comes from and how every event is extracted
from it. ``pyproject.toml`` registers that file with MEDS-Extract's
``MEDS_extract.pipelines`` entry-point group under the name ``MIMIC-IV``, so the ETL runs
as::

    meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR

It also ships ``MIMIC_IV_MEDS/sumeds_rules.yaml``, registered with suMEDS's ``sumeds.rules``
entry-point group under the same name, which the config's ``sumeds_enrich`` post-stage uses to
enrich the code metadata at the end of that run. The only code is two small stages the config
declares, registered in MEDS-Transforms's ``MEDS_transforms.stages`` entry-point group:
``unit_keys.py``, the ``mimic_iv_unit_keys`` pre-stage that finds the infusion itemids recorded in
more than one unit (whose codes end in it), and ``unit_metadata.py``, the ``mimic_iv_unit_metadata``
post-stage, which fills the units item labels state and links codes that differ only by unit.

This package exists so the YAML is addressable as package data —
``importlib.resources.files("MIMIC_IV_MEDS")`` — from an installed wheel. Nothing here is
importable API; deliberately, there is nothing to import.
"""
