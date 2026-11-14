"""Build one MEDS dataset from MIMIC-IV plus any combination of its linked PhysioNet modules.

ED and notes become ordinary MEDS events (text in `text_value`). ECGs, echo DICOMs, chest
radiographs and waveform windows become one event per object: `external_value_link` (declared as
a MEDS `additional_value_modality_columns` column) points at the object, stored inside the MEDS
root under `external_values/`, and `external_value_encoding` says how to read it
(`external_values.py`). Entry point: `mimic-iv-meds-multimodal` (`cli.py`).
"""
