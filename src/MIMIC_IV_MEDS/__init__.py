"""The MIMIC-IV MEDS ETL — a configuration package, not a library.

This distribution ships no code. Its entire content is
``MIMIC_IV_MEDS/event_configs.yaml``, a MESSY (MEDS-Extract Specification Syntax YAML)
document declaring where the raw MIMIC-IV data comes from and how every event is extracted
from it. ``pyproject.toml`` registers that file with MEDS-Extract's
``MEDS_extract.pipelines`` entry-point group under the name ``MIMIC-IV``, so the ETL runs
as::

    meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR

It also ships ``MIMIC_IV_MEDS/sumeds_rules.yaml``, registered with suMEDS's ``sumeds.rules``
entry-point group under the same name, so ``suMEDS-extract spec=MIMIC-IV
output_dir=$MEDS_OUTPUT_DIR`` runs the ETL and then enriches its code metadata.

This package exists so the YAML is addressable as package data —
``importlib.resources.files("MIMIC_IV_MEDS")`` — from an installed wheel. Nothing here is
importable API; deliberately, there is nothing to import.
"""
