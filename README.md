# MIMIC-IV MEDS Extraction ETL

[![PyPI - Version](https://img.shields.io/pypi/v/MIMIC-IV-MEDS)](https://pypi.org/project/MIMIC-IV-MEDS/)
[![codecov](https://codecov.io/gh/Medical-Event-Data-Standard/MIMIC_IV_MEDS/graph/badge.svg?token=E7H6HKZV3O)](https://codecov.io/gh/Medical-Event-Data-Standard/MIMIC_IV_MEDS)
[![tests](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/actions/workflows/tests.yaml/badge.svg)](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/actions/workflows/tests.yml)
[![code-quality](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/actions/workflows/code-quality-main.yaml/badge.svg)](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/actions/workflows/code-quality-main.yaml)
![python](https://img.shields.io/badge/-Python_3.11-blue?logo=python&logoColor=white)
[![license](https://img.shields.io/badge/License-MIT-green.svg?labelColor=gray)](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS#license)
[![PRs](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/pulls)
[![contributors](https://img.shields.io/github/contributors/Medical-Event-Data-Standard/MIMIC_IV_MEDS.svg)](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/graphs/contributors)

This pipeline extracts the MIMIC-IV dataset (from physionet) into the MEDS format.

## Usage:

```bash
pip install MIMIC_IV_MEDS
export DATASET_DOWNLOAD_USERNAME=$PHYSIONET_USERNAME
export DATASET_DOWNLOAD_PASSWORD=$PHYSIONET_PASSWORD
MEDS_extract-MIMIC_IV root_output_dir=$ROOT_OUTPUT_DIR
```

When you run this, the program will:

1. Download the needed raw MIMIC files for the currently supported version into
    `$ROOT_OUTPUT_DIR/raw_input`.
2. Perform initial, pre-MEDS processing on the raw MIMIC files, saving the results in
    `$ROOT_OUTPUT_DIR/pre_MEDS`.
3. Construct the final MEDS cohort, and save it to `$ROOT_OUTPUT_DIR/MEDS_cohort`.

You can also specify the target directories more directly, with

```bash
export DATASET_DOWNLOAD_USERNAME=$PHYSIONET_USERNAME
export DATASET_DOWNLOAD_PASSWORD=$PHYSIONET_PASSWORD
MEDS_extract-MIMIC_IV raw_input_dir=$RAW_INPUT_DIR pre_MEDS_dir=$PRE_MEDS_DIR MEDS_cohort_dir=$MEDS_COHORT_DIR
```

## Examples and More Info:

You can run `MEDS_extract-MIMIC_IV --help` for more information on the arguments and options. You can also run

```bash
MEDS_extract-MIMIC_IV root_output_dir=$ROOT_OUTPUT_DIR do_demo=True
```

to run the entire pipeline over the publicly available, fully open MIMIC-IV demo dataset.

## Expected runtime and compute needs

This pipeline can be successfully run over the full MIMIC-IV on a 5-core machine leveraging around 165GB of
memory in approximately 7 hours (note this time includes the time to download all of the MIMIC-IV files as
well, and this test was run on a machine with poor network transfer speeds and without any parallelization
applied to the transformation steps, so these speeds can likely be greatly increased). The output folder of
data is 9.8 GB. This can be reduced significantly as well as intermediate files not necessary for the final
MEDS dataset are retained in additional folders. See
[this github issue](https://github.com/Medical-Event-Data-Standard/MEDS_transforms/issues/235) for tracking on ensuring these
directories are automatically cleaned up in the future.

## 📚 Citing this work

If you use this software in your research, please cite it! You can use the **"Cite this repository"** button on GitHub.

The citation information is maintained in the `CITATION.cff` file in this repository.

## 🔧 Common Issues / FAQ

### ❓ Issue: `FileNotFoundError` or pipeline errors during the `pre_MEDS` step on Ubuntu (symlinks not recognized)

#### Problem:

Some users running the pipeline encounter errors during the `pre_MEDS` step, where the scripts attempt to **create symlinks** but later fails to recognize or access them — even though the symlinks appear to exist in the file system.

#### Solution:

A `do_copy=True` option is available in the CLI that allows the pipeline to **copy files instead of symlinking**, avoiding this issue entirely (at the cost of additional disk usage). You can enable this by adding `do_copy=True` to your command:

```bash
MEDS_extract-MIMIC_IV root_output_dir=$ROOT_OUTPUT_DIR do_copy=True
```
