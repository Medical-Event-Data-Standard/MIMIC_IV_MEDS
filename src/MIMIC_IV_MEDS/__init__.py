"""The MIMIC-IV MEDS ETL — a configuration package, not a library.

This distribution ships no code. Its entire content is
``MIMIC_IV_MEDS/event_configs.yaml``, a MESSY (MEDS-Extract Specification Syntax YAML)
document declaring where the raw MIMIC-IV data comes from and how every event is extracted
from it. ``pyproject.toml`` registers that file with MEDS-Extract's
``MEDS_extract.pipelines`` entry-point group under the name ``MIMIC-IV``, so the ETL runs
as::

    meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR

It also ships ``MIMIC_IV_MEDS/sumeds_rules.yaml``, registered with suMEDS's ``sumeds.rules``
entry-point group under the same name, which the config's ``sumeds_enrich`` post-stage uses to
enrich the code metadata at the end of that run.

This package exists so the YAML is addressable as package data —
``importlib.resources.files("MIMIC_IV_MEDS")`` — from an installed wheel. The one piece of code is
``MIMIC_IV_MEDS.multimodal`` (the ``mimic-iv-meds-multimodal`` command), which combines MIMIC-IV
with its linked modules and stores their images and waveforms inside the MEDS root; it is a
command, not a library API.
"""
