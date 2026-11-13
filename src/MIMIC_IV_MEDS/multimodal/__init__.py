"""Build one MEDS dataset from MIMIC-IV plus any combination of its linked PhysioNet modules.

ED and notes become ordinary MEDS events (text in `text_value`). ECGs, echo DICOMs and chest
radiographs become one event per object whose link column (`ecg_link`, `echo_link`, `cxr_link`,
declared as MEDS `additional_value_modality_columns`) points at the object, stored inside the MEDS
root under its SHA-256. Entry point: `mimic-iv-meds-multimodal` (`cli.py`).
"""
