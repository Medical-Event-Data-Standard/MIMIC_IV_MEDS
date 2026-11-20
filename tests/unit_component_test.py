"""MIMIC-IV codes that end in a unit, tested with suMEDS's shared `UnitComponentContract`.

The contract finds every code of the spec whose last component is a unit and evaluates it with
dftly, as MEDS-Extract does: the component is the unit stripped, `UNK` where a row records none
or a blank one, and the `unit` metadata stays the raw column. `expected_unit_events` pins the
events it finds, so a new code that ends in a unit is a conscious change here.
"""

from sumeds.testing.contracts import UnitComponentContract


class TestUnitComponents(UnitComponentContract):
    spec = "MIMIC-IV"
    expected_unit_events = (
        "hosp/labevents/lab_result",
        "icu/chartevents/event",
        "icu/inputevents/input_start",
        "icu/inputevents/input_end",
        "icu/outputevents/output",
    )
