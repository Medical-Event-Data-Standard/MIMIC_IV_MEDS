"""MIMIC-IV codes that end in a unit, tested with suMEDS's shared `UnitComponentContract`.

The contract finds every code of the spec whose last component is a unit and evaluates it with
dftly, as MEDS-Extract does: the component is the unit stripped, `UNK` where a row records none
or a blank one, and the `unit` metadata stays the raw column. `expected_unit_events` pins the
events it finds, so a new code that ends in a unit is a conscious change here.
`require_unit_in_code` checks the other direction: every event whose `_metadata._self.unit` reads a
row column gives that unit in its code.
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
    require_unit_in_code = True


# The extension specs carry the base's events unchanged and add none that ends in a unit or reads its
# `unit` from a row column, so the same events are expected in each.
class TestUnitComponentsMNE(TestUnitComponents):
    spec = "MIMIC-IV-MNE"


class TestUnitComponentsED(TestUnitComponents):
    spec = "MIMIC-IV-ED"


class TestUnitComponentsCXRDB(TestUnitComponents):
    spec = "MIMIC-IV-CXRDB"
