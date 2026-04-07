from importlib.metadata import PackageNotFoundError, version
from importlib.resources import files

from omegaconf import OmegaConf

__package_name__ = "MIMIC_IV_MEDS"
try:
    __version__ = version(__package_name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"

MAIN_CFG = files(__package_name__).joinpath("configs/main.yaml")
EVENT_CFG = files(__package_name__).joinpath("configs/event_configs.yaml")
ETL_CFG = files(__package_name__).joinpath("configs/ETL.yaml")
PRE_MEDS_PY = files(__package_name__).joinpath("pre_MEDS.py")
PRE_MEDS_CFG = files(__package_name__).joinpath("configs/pre_MEDS.yaml")
DATASET_CFG = files(__package_name__).joinpath("dataset.yaml")

dataset_info = OmegaConf.load(DATASET_CFG)

HAS_PRE_MEDS = PRE_MEDS_PY.exists()

event_config = OmegaConf.load(EVENT_CFG)

__all__ = [
    "DATASET_CFG",
    "ETL_CFG",
    "EVENT_CFG",
    "HAS_PRE_MEDS",
    "MAIN_CFG",
    "PRE_MEDS_CFG",
    "__package_name__",
    "__version__",
    "dataset_info",
    "event_config",
]
