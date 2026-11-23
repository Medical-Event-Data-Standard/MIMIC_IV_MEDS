# Sources and citations

Every dataset and published work this ETL reads or relies on, with its version, access terms and
citation. If you publish results from a MEDS cohort built here, cite this ETL **and** every dataset
the spec you ran reads. Each PhysioNet project also asks you to cite PhysioNet itself (see
[PhysioNet](#physionet)).

DOIs below are version-specific, as listed on each PhysioNet page and checked against the DOI
registry. PhysioNet also lists a "latest version" DOI for each project; use the versioned one for
reproducibility.

## Which specs read what

| Source                                                    | `MIMIC-IV` | `MIMIC-IV` demo | `MIMIC-IV-MNE` | MIMIC-IV-ED *(in progress)* | MIMIC-CXR-DB *(in progress)* |
| --------------------------------------------------------- | :--------: | :-------------: | :------------: | :-------------------------: | :--------------------------: |
| [MIMIC-IV 3.1](#mimic-iv)                                 |     ✓      |                 |       ✓        |              ✓              |              ✓               |
| [MIMIC-IV Clinical Database Demo 2.2](#mimic-iv-demo)     |            |        ✓        |                |                             |                              |
| [mimic-code concept maps](#mimic-code)                    |     ✓      |        ✓        |       ✓        |              ✓              |              ✓               |
| [MIMIC-Note-Extract 1.0](#mimic-note-extract)             |            |                 |       ✓        |                             |                              |
| [MIMIC-IV-Note 2.2](#mimic-iv-note) (indirectly, via MNE) |            |                 |       ✓        |                             |                              |
| [MIMIC-IV-ED 2.2](#mimic-iv-ed)                           |            |                 |                |              ✓              |                              |
| [MIMIC-CXR-DB 1.0](#mimic-cxr-db)                         |            |                 |                |                             |              ✓               |

## This ETL

| Field    | Value                                                                                     |
| -------- | ----------------------------------------------------------------------------------------- |
| Software | MIMIC-IV MEDS                                                                             |
| Code     | <https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS>                            |
| Licence  | MIT                                                                                       |
| DOI      | [10.5281/zenodo.17535579](https://doi.org/10.5281/zenodo.17535579) (all versions, Zenodo) |

The authoritative citation metadata is
[`CITATION.cff`](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/blob/main/CITATION.cff);
GitHub's "Cite this repository" button renders it.

```bibtex
@software{mimic_iv_meds,
  author    = {McDermott, Matthew and Xu, Justin},
  title     = {{MIMIC-IV MEDS}: An {ETL} pipeline to extract {MIMIC-IV} data into the {MEDS} format},
  publisher = {Zenodo},
  doi       = {10.5281/zenodo.17535579},
  url       = {https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS}
}
```

## MIMIC-IV

| Field    | Value                                                                                                                                 |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| Version  | 3.1 (published 11 October 2024); pinned in `sources.dataset_version.dataset`                                                          |
| URL      | <https://physionet.org/content/mimiciv/3.1/>                                                                                          |
| DOI      | [10.13026/kpb9-mt58](https://doi.org/10.13026/kpb9-mt58)                                                                              |
| Access   | Credentialed PhysioNet users; CITI "Data or Specimens Only Research" training; PhysioNet Credentialed Health Data Use Agreement 1.5.0 |
| Licence  | PhysioNet Credentialed Health Data License 1.5.0                                                                                      |
| Used for | Every table of the base ETL (`hosp/`, `icu/`)                                                                                         |

```bibtex
@misc{johnson2024mimiciv,
  author    = {Johnson, Alistair and Bulgarelli, Lucas and Pollard, Tom and Gow, Brian and Moody, Benjamin and Horng, Steven and Celi, Leo Anthony and Mark, Roger},
  title     = {{MIMIC-IV} (version 3.1)},
  publisher = {PhysioNet},
  year      = {2024},
  doi       = {10.13026/kpb9-mt58},
  url       = {https://physionet.org/content/mimiciv/3.1/}
}

@article{johnson2023mimiciv,
  author  = {Johnson, Alistair E. W. and Bulgarelli, Lucas and Shen, Lu and Gayles, Alvin and Shammout, Ayad and Horng, Steven and Pollard, Tom J. and Hao, Sicheng and Moody, Benjamin and Gow, Brian and Lehman, Li-wei H. and Celi, Leo A. and Mark, Roger G.},
  title   = {{MIMIC-IV}, a freely accessible electronic health record dataset},
  journal = {Scientific Data},
  volume  = {10},
  number  = {1},
  year    = {2023},
  doi     = {10.1038/s41597-022-01899-x}
}
```

## MIMIC-IV demo

| Field    | Value                                                                     |
| -------- | ------------------------------------------------------------------------- |
| Name     | MIMIC-IV Clinical Database Demo                                           |
| Version  | 2.2 (published 31 January 2023); pinned in `sources.dataset_version.demo` |
| URL      | <https://physionet.org/content/mimic-iv-demo/2.2/>                        |
| DOI      | [10.13026/dp1f-ex47](https://doi.org/10.13026/dp1f-ex47)                  |
| Access   | Open access: anyone, under the licence terms; no credentials              |
| Licence  | Open Data Commons Open Database License (ODbL) v1.0                       |
| Used for | `dataset_key=demo` runs, CI, and the regression baseline (100 subjects)   |

```bibtex
@misc{johnson2023mimicivdemo,
  author    = {Johnson, Alistair and Bulgarelli, Lucas and Pollard, Tom and Horng, Steven and Celi, Leo Anthony and Mark, Roger},
  title     = {{MIMIC-IV} Clinical Database Demo (version 2.2)},
  publisher = {PhysioNet},
  year      = {2023},
  doi       = {10.13026/dp1f-ex47},
  url       = {https://physionet.org/content/mimic-iv-demo/2.2/}
}
```

## mimic-code

| Field    | Value                                                                                                                                                 |
| -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| What     | Ten concept-map CSVs from `mimic-iv/concepts/concept_map/` (e.g. `d_labitems_to_loinc.csv`, `meas_chartevents_main.csv`, `inputevents_to_rxnorm.csv`) |
| Version  | Git tag `v2.4.0`; each file is pinned by SHA-256 in the `common` source bucket                                                                        |
| URL      | <https://github.com/MIT-LCP/mimic-code>                                                                                                               |
| Licence  | MIT                                                                                                                                                   |
| Used for | Lab descriptions, and LOINC / SNOMED / RxNorm parent codes for lab, chart, input, output and procedure items                                          |

```bibtex
@article{johnson2018mimiccode,
  author  = {Johnson, Alistair E. W. and Stone, David J. and Celi, Leo A. and Pollard, Tom J.},
  title   = {The {MIMIC} Code Repository: enabling reproducibility in critical care research},
  journal = {Journal of the American Medical Informatics Association},
  volume  = {25},
  number  = {1},
  pages   = {32--39},
  year    = {2018},
  doi     = {10.1093/jamia/ocx084}
}
```

## MIMIC-Note-Extract

| Field    | Value                                                                                                                                                          |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Name     | MIMIC-IV-Ext MIMIC Note Extract: A Large-Scale Structured Dataset Extracted from MIMIC-IV-Note Discharge Summaries                                             |
| Version  | 1.0 (published 21 October 2026)                                                                                                                                |
| URL      | <https://physionet.org/content/mimic-iv-ext-note-extract/1.0/>                                                                                                 |
| DOI      | [10.13026/cq50-rd11](https://doi.org/10.13026/cq50-rd11)                                                                                                       |
| Access   | Credentialed PhysioNet users; CITI "Data or Specimens Only Research" training; PhysioNet Credentialed Health Data Use Agreement 1.5.0, signed for this project |
| Licence  | PhysioNet Credentialed Health Data License 1.5.0                                                                                                               |
| Paper    | arXiv:2611.04685, accepted at ML4H 2026 (Proceedings Track)                                                                                                    |
| Used for | [`MIMIC-IV-MNE`](extensions/mne.md)                                                                                                                            |

Note that the dataset and the paper list the first two authors in a different order.

```bibtex
@misc{kuntzer2026mne,
  author    = {Kuntzer, Jonas and Schwethelm, Kristian and Ketzer, Conrad and Wende, Eva and Schlager, Moritz and Rueckert, Daniel and Kaissis, Georgios and Jungmann, Friederike and Hager, Paul},
  title     = {{MIMIC-IV-Ext} {MIMIC} Note Extract: A Large-Scale Structured Dataset Extracted from {MIMIC-IV-Note} Discharge Summaries (version 1.0)},
  publisher = {PhysioNet},
  year      = {2026},
  doi       = {10.13026/cq50-rd11},
  url       = {https://physionet.org/content/mimic-iv-ext-note-extract/1.0/}
}

@misc{schwethelm2026mne,
  author        = {Schwethelm, Kristian and Kuntzer, Jonas and Ketzer, Conrad and Wende, Eva and Schlager, Moritz and Rueckert, Daniel and Kaissis, Georgios and Jungmann, Friederike and Hager, Paul},
  title         = {{MIMIC-Note-Extract}: A Large-Scale Structured Dataset Extracted from {MIMIC-IV-Note} Discharge Summaries},
  year          = {2026},
  eprint        = {2611.04685},
  archivePrefix = {arXiv},
  primaryClass  = {cs.CL},
  doi           = {10.48550/arXiv.2611.04685}
}
```

## MIMIC-IV-Note

MNE is derived from these discharge summaries. This ETL does not download MIMIC-IV-Note, but
`spec=MIMIC-IV-MNE` output is a derivative of it, so cite it alongside MNE.

| Field   | Value                                                                                                                                 |
| ------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| Name    | MIMIC-IV-Note: Deidentified free-text clinical notes                                                                                  |
| Version | 2.2 (published 6 January 2023)                                                                                                        |
| URL     | <https://physionet.org/content/mimic-iv-note/2.2/>                                                                                    |
| DOI     | [10.13026/1n74-ne17](https://doi.org/10.13026/1n74-ne17)                                                                              |
| Access  | Credentialed PhysioNet users; CITI "Data or Specimens Only Research" training; PhysioNet Credentialed Health Data Use Agreement 1.5.0 |
| Licence | PhysioNet Credentialed Health Data License 1.5.0                                                                                      |

```bibtex
@misc{johnson2023mimicivnote,
  author    = {Johnson, Alistair and Pollard, Tom and Horng, Steven and Celi, Leo Anthony and Mark, Roger},
  title     = {{MIMIC-IV-Note}: Deidentified free-text clinical notes (version 2.2)},
  publisher = {PhysioNet},
  year      = {2023},
  doi       = {10.13026/1n74-ne17},
  url       = {https://physionet.org/content/mimic-iv-note/2.2/}
}
```

## MIMIC-IV-ED

*Used by the MIMIC-IV-ED extension, which is in progress
([#74](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/issues/74)). Its page will
document how it is used.*

| Field   | MIMIC-IV-ED                                                                           | MIMIC-IV-ED Demo                                         |
| ------- | ------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| Version | 2.2 (published 5 January 2023)                                                        | 2.2 (published 8 February 2023)                          |
| URL     | <https://physionet.org/content/mimic-iv-ed/2.2/>                                      | <https://physionet.org/content/mimic-iv-ed-demo/2.2/>    |
| DOI     | [10.13026/5ntk-km72](https://doi.org/10.13026/5ntk-km72)                              | [10.13026/jzz5-vs76](https://doi.org/10.13026/jzz5-vs76) |
| Access  | Credentialed; CITI training; PhysioNet Credentialed Health DUA 1.5.0 for this project | Open access                                              |
| Licence | PhysioNet Credentialed Health Data License 1.5.0                                      | ODbL v1.0                                                |

```bibtex
@misc{johnson2023mimicived,
  author    = {Johnson, Alistair and Bulgarelli, Lucas and Pollard, Tom and Celi, Leo Anthony and Mark, Roger and Horng, Steven},
  title     = {{MIMIC-IV-ED} (version 2.2)},
  publisher = {PhysioNet},
  year      = {2023},
  doi       = {10.13026/5ntk-km72},
  url       = {https://physionet.org/content/mimic-iv-ed/2.2/}
}

@misc{johnson2023mimicived_demo,
  author    = {Johnson, Alistair and Bulgarelli, Lucas and Pollard, Tom and Celi, Leo Anthony and Horng, Steven and Mark, Roger},
  title     = {{MIMIC-IV-ED} Demo (version 2.2)},
  publisher = {PhysioNet},
  year      = {2023},
  doi       = {10.13026/jzz5-vs76},
  url       = {https://physionet.org/content/mimic-iv-ed-demo/2.2/}
}
```

## MIMIC-CXR-DB

*Used by the MIMIC-CXR-DB extension, which is in progress. Its page will document which of these
sources it reads.*

MIMIC-CXR-Ext-MIMIC-CXR-DB links MIMIC-CXR chest radiographs to MIMIC-IV hospital admissions. Its
PhysioNet page lists it as derived from MIMIC-CXR 2.1.0, MIMIC-CXR-JPG 2.1.0, MIMIC-IV 3.1 and
MIMIC-IV-Note 2.2.

| Field   | MIMIC-CXR-Ext-MIMIC-CXR-DB                                      | MIMIC-CXR Database                                       | MIMIC-CXR-JPG                                            |
| ------- | --------------------------------------------------------------- | -------------------------------------------------------- | -------------------------------------------------------- |
| Version | 1.0 (published 1 October 2026)                                  | 2.1.0 (published 23 July 2024)                           | 2.1.0 (published 12 March 2024)                          |
| URL     | <https://physionet.org/content/mimic-cxr-ext-mimic-cxr-db/1.0/> | <https://physionet.org/content/mimic-cxr/2.1.0/>         | <https://physionet.org/content/mimic-cxr-jpg/2.1.0/>     |
| DOI     | [10.13026/gy0s-e371](https://doi.org/10.13026/gy0s-e371)        | [10.13026/4jqj-jw95](https://doi.org/10.13026/4jqj-jw95) | [10.13026/jsn5-t979](https://doi.org/10.13026/jsn5-t979) |
| Access  | Credentialed; CITI training; DUA 1.5.0 for this project         | Credentialed; CITI training; DUA 1.5.0                   | Credentialed; CITI training; DUA 1.5.0                   |
| Licence | PhysioNet Credentialed Health Data License 1.5.0                | PhysioNet Credentialed Health Data License 1.5.0         | PhysioNet Credentialed Health Data License 1.5.0         |

The MIMIC-CXR-DB authors also ask for a citation of their paper: Turki *et al.* (2026),
"MIMIC-CXR-DB: An Admission-Linked, Ontology-Grounded Multimodal Resource Built on MIMIC-CXR and
MIMIC-IV", in *Multimodal Learning with Medical Tabular Data (MultiTab@MICCAI 2026)*, Springer. Its
DOI is not yet listed; check the PhysioNet page for the current reference.

```bibtex
@misc{turki2026mimiccxrdb,
  author    = {Turki, Houcemeddine and Ismaila, Lukman and Ben Salem, Ahmed and Nebli, Ahmed and Kchaou, Mahdi and Dere, Abdulhameed and Alzahrani, Anas and Koubaa, Makram},
  title     = {{MIMIC-CXR-Ext-MIMIC-CXR-DB}: Linking {MIMIC-IV} Chest Radiographs with Rich and Harmonized Clinical Context (version 1.0)},
  publisher = {PhysioNet},
  year      = {2026},
  doi       = {10.13026/gy0s-e371},
  url       = {https://physionet.org/content/mimic-cxr-ext-mimic-cxr-db/1.0/}
}

@misc{johnson2024mimiccxr,
  author    = {Johnson, Alistair and Pollard, Tom and Mark, Roger and Berkowitz, Seth and Horng, Steven},
  title     = {{MIMIC-CXR} Database (version 2.1.0)},
  publisher = {PhysioNet},
  year      = {2024},
  doi       = {10.13026/4jqj-jw95},
  url       = {https://physionet.org/content/mimic-cxr/2.1.0/}
}

@article{johnson2019mimiccxr,
  author  = {Johnson, Alistair E. W. and Pollard, Tom J. and Berkowitz, Seth J. and Greenbaum, Nathaniel R. and Lungren, Matthew P. and Deng, Chih-ying and Mark, Roger G. and Horng, Steven},
  title   = {{MIMIC-CXR}, a de-identified publicly available database of chest radiographs with free-text reports},
  journal = {Scientific Data},
  volume  = {6},
  number  = {1},
  pages   = {317},
  year    = {2019},
  doi     = {10.1038/s41597-019-0322-0}
}

@misc{johnson2024mimiccxrjpg,
  author    = {Johnson, Alistair and Lungren, Matthew and Peng, Yifan and Lu, Zhiyong and Mark, Roger and Berkowitz, Seth and Horng, Steven},
  title     = {{MIMIC-CXR-JPG} - chest radiographs with structured labels (version 2.1.0)},
  publisher = {PhysioNet},
  year      = {2024},
  doi       = {10.13026/jsn5-t979},
  url       = {https://physionet.org/content/mimic-cxr-jpg/2.1.0/}
}
```

## PhysioNet

Every PhysioNet project above asks users to include PhysioNet's standard citation:

```bibtex
@article{pollard2026physionet,
  author  = {Pollard, Tom and Moody, Benjamin E. and Lehman, Li-wei H. and Gow, Brian J. and Fernandes, Chrystinne and Xie, Chen and Johnson, Alistair and Mark, Roger G. and Heldt, Thomas},
  title   = {{PhysioNet} as a global platform for biomedical research},
  journal = {Nature Health},
  volume  = {1},
  number  = {8},
  pages   = {792--795},
  year    = {2026},
  doi     = {10.1038/s44360-026-00096-z}
}
```

## Other works used

| Work                                                                                                                            | Used for                                                                                                                                                                                       | Licence / terms    |
| ------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------ |
| [MIMIC-IV documentation](https://mimic.mit.edu/docs/iv/) (source: MIT-LCP/mimic-website)                                        | Paraphrased in the `_self` code descriptions, which name the source column in parentheses                                                                                                      | MIT                |
| [suMEDS](https://github.com/rvandewater/SumMEDS)                                                                                | The `sumeds_enrich` post-stage and `suMEDS-enrich`, which add external-ontology links to `codes.parquet` from `sumeds_rules.yaml`, using suMEDS's versioned ontology packs (sumeds-ontologies) | MIT (the software) |
| [MEDS-Extract](https://github.com/mmcdermott/MEDS_extract) and [MEDS-Transforms](https://github.com/mmcdermott/MEDS_transforms) | The engine that runs the spec                                                                                                                                                                  | MIT                |
| [dftly](https://github.com/mmcdermott/dftly)                                                                                    | The expression language used in the spec                                                                                                                                                       | MIT                |
| MEDS (below)                                                                                                                    | The output format                                                                                                                                                                              | —                  |

```bibtex
@inproceedings{meds2024,
  author    = {{MEDS Working Group} and Arnrich, Bert and Choi, Edward and Fries, Jason Alan and McDermott, Matthew B. A. and Oh, Jungwoo and Pollard, Tom and Shah, Nigam and Steinberg, Ethan and Wornow, Michael and van de Water, Robin},
  title     = {Medical Event Data Standard ({MEDS}): Facilitating Machine Learning for Health},
  booktitle = {ICLR 2024 Workshop on Learning from Time Series For Health},
  year      = {2024},
  url       = {https://openreview.net/forum?id=IsHy2ebjIG}
}
```
