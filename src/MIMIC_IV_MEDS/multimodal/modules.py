"""The extensions a multimodal build can put on top of MIMIC-IV, and how their releases are found.

An *extension* (internally a `Module`) is one PhysioNet project family that links to MIMIC-IV:
MIMIC-IV-ED, MIMIC-IV-Note, MIMIC-Note-Extract, MIMIC-IV-ECG, MIMIC-IV-ECHO, MIMIC-CXR-JPG,
MIMIC-CXR-DB and the MIMIC-IV Waveform Database. On the command line it is named
`mimic-iv-<x>` (`--extensions mimic-iv-mne,mimic-iv-cxr`); its PhysioNet slug and its short
internal name are accepted too. Each has one or more *releases* (the open demo, the credentialed
full release), told apart by project slug. A release is found under any of the build's `--root`
folders in one of these layouts:

- `<root>/<slug>/<version>/` (and `<root>/physionet.org/files/<slug>/<version>/`): what
  `wget -r` or the "files" browser give;
- `<root>/<slug>[-<title words>]-<version>/`: what the "Download the ZIP file" button and the
  `physionet` package (so `mimic-demo-mm download`, the multimodal demo downloader) give, e.g.
  `mimic-iv-demo-2.2/`, `mimic-iv-clinical-database-demo-2.2/` or
  `mimic-iv-ecg-demo-diagnostic-electrocardiogram-matched-subset-demo-0.1/`;
- `<root>/<slug>/` holding `SYNTHETIC_<slug>.json`: a **simulated** release (this repository's
  `data/synthetic_demo/`), used only when no real release of the extension is found and flagged
  as synthetic in every piece of metadata.

A candidate directory only counts when it holds the release's marker file (and everything in
`requires`), so an unrelated folder with a matching name, or the tables-only copy of a release the
demo downloader makes, is ignored. `--path <extension>=<dir>` (and `--version`) bypass the search.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

ODBL = "ODbL-1.0"
PHYSIONET_CREDENTIALED = "PhysioNet Credentialed Health Data License 1.5.0"

_VERSION = re.compile(r"\d+(?:\.\d+)+")
_TRAILING_VERSION = re.compile(r"-(\d+(?:\.\d+)+)$")


@dataclass(frozen=True)
class Release:
    """One PhysioNet project that provides a module's files."""

    project: str  # the PhysioNet slug, e.g. "mimic-iv-ed-demo"
    demo: bool
    license: str
    #: Other directory-name prefixes the release ships under (the core demo's ZIP is named
    #: `mimic-iv-clinical-database-demo-<version>`, not after its slug).
    aliases: tuple[str, ...] = ()

    @property
    def credentialed(self) -> bool:
        return self.license == PHYSIONET_CREDENTIALED

    @property
    def url(self) -> str:
        return f"https://physionet.org/content/{self.project}/"


@dataclass(frozen=True)
class Part:
    """A selectable piece of a module (`--modules echo:measurements`).

    `tables` are the fragment tables only this part feeds (left out of the spec when it is not built);
    empty when the part selects rows of a shared table instead (the note types).
    """

    name: str
    title: str
    tables: tuple[str, ...] = ()
    link_column: str | None = None
    raw_source_id_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class Module:
    name: str  # the CLI name, e.g. "ecg"
    label: str  # how it appears in `dataset_name`, e.g. "ECG"
    title: str
    releases: tuple[Release, ...]
    #: A path, relative to the release root, that must exist for a directory to count as this release.
    marker: str | tuple[str, ...]
    #: The MEDS column holding paths to this module's objects (images, waveforms), if any.
    link_column: str | None = None
    #: Extension columns this module adds that are join keys into its raw release.
    raw_source_id_columns: tuple[str, ...] = ()
    #: Selectable parts; empty when the module is all-or-nothing.
    parts: tuple[Part, ...] = ()
    #: The name on the command line and in the metadata, e.g. "mimic-iv-ecg".
    extension: str = ""
    #: Other names accepted on the command line (the PhysioNet slug, older short names).
    aliases: tuple[str, ...] = ()
    #: The spec fragment with this module's tables, relative to the package.
    fragment: str = ""
    #: Paths (relative to the release root) that must ALL exist, besides one of the markers: the ECG
    #: release only counts with its `files/`, not as the tables-only copy the demo downloader makes.
    requires: tuple[str, ...] = ()
    #: `{other module: [(table, event), ...]}`: events this module leaves out when the other module
    #: is built too, because the other one already emits them (CXR-DB's image events, when the
    #: MIMIC-CXR-JPG images are in, with their links).
    yields_to: dict[str, tuple[tuple[str, str], ...]] = field(default_factory=dict)

    def __hash__(self) -> int:
        return hash(self.name)

    @property
    def names(self) -> tuple[str, ...]:
        """Every name the module is accepted under, canonical first."""
        return tuple(dict.fromkeys((self.extension or self.name, *self.aliases, self.name)))

    def markers(self) -> tuple[str, ...]:
        return (self.marker,) if isinstance(self.marker, str) else self.marker

    @property
    def part_names(self) -> tuple[str, ...]:
        return tuple(p.name for p in self.parts)


@dataclass
class FoundRelease:
    module: Module
    release: Release
    version: str
    root: Path
    #: The parts asked for (all of the module's when none were named).
    parts: tuple[str, ...] = ()
    #: Whether the parts were named explicitly; a named part whose data is missing is an error.
    explicit: bool = False
    extras: dict = field(default_factory=dict)
    #: A simulated stand-in (`SYNTHETIC_<slug>.json`), not the PhysioNet release.
    synthetic: bool = False

    def __post_init__(self):
        if not self.parts:
            self.parts = self.module.part_names

    @property
    def tag(self) -> str:
        """`<slug>/<version>`: how the release is named in `dataset_version`."""
        return f"{self.release.project}/{self.version}"

    @property
    def label(self) -> str:
        """The module's label in `dataset_name`, with its parts when only some were asked for."""
        label = self.module.label
        if self.explicit and set(self.parts) != set(self.module.part_names):
            label = f"{label}({'+'.join(self.parts)})"
        return f"{label}[synthetic]" if self.synthetic else label

    @property
    def license(self) -> str:
        """The release's license; a simulated release states its own (ODbL)."""
        return self.extras.get("license") or self.release.license

    @property
    def credentialed(self) -> bool:
        return self.license == PHYSIONET_CREDENTIALED

    def wants(self, part: str) -> bool:
        return part in self.parts


CORE = Module(
    name="core",
    label="MIMIC-IV",
    title="MIMIC-IV (hosp and icu modules)",
    releases=(
        Release("mimic-iv-demo", True, ODBL, aliases=("mimic-iv-clinical-database-demo",)),
        Release("mimiciv", False, PHYSIONET_CREDENTIALED, aliases=("mimic-iv",)),
    ),
    marker="hosp/patients.csv.gz",
)

MODULES: dict[str, Module] = {
    m.name: m
    for m in (
        Module(
            name="ed",
            extension="mimic-iv-ed",
            label="ED",
            title="MIMIC-IV-ED (emergency department stays, triage, vitals, medications)",
            releases=(
                Release("mimic-iv-ed-demo", True, ODBL),
                Release("mimic-iv-ed", False, PHYSIONET_CREDENTIALED),
            ),
            marker="ed/edstays.csv.gz",
            raw_source_id_columns=("ed_stay_id",),
            fragment="extensions/ed.yaml",
        ),
        Module(
            name="note",
            extension="mimic-iv-note",
            label="Note",
            title="MIMIC-IV-Note (discharge summaries and radiology reports, as text)",
            releases=(Release("mimic-iv-note", False, PHYSIONET_CREDENTIALED),),
            marker=("note/discharge.csv.gz", "note/radiology.csv.gz"),
            raw_source_id_columns=("note_id",),
            parts=(
                Part("discharge", "discharge summaries and their addenda (note/discharge.csv.gz)"),
                Part("radiology", "radiology reports and their addenda (note/radiology.csv.gz)"),
            ),
            fragment="multimodal/fragments/note.yaml",
        ),
        Module(
            name="mne",
            extension="mimic-iv-mne",
            aliases=("mimic-iv-ext-note-extract",),
            label="MNE",
            title="MIMIC-Note-Extract (13 discharge-summary sections as structured tables)",
            releases=(Release("mimic-iv-ext-note-extract", False, PHYSIONET_CREDENTIALED),),
            marker=("allergies.csv.gz", "allergies.csv"),
            fragment="extensions/mne.yaml",
        ),
        Module(
            name="ecg",
            extension="mimic-iv-ecg",
            label="ECG",
            title="MIMIC-IV-ECG (diagnostic 12-lead ECGs, WFDB)",
            releases=(
                Release("mimic-iv-ecg-demo", True, ODBL),
                Release("mimic-iv-ecg", False, ODBL),
            ),
            marker="record_list.csv",
            requires=("files",),
            link_column="ecg_link",
            raw_source_id_columns=("ecg_study_id",),
            fragment="multimodal/fragments/ecg.yaml",
        ),
        Module(
            name="echo",
            extension="mimic-iv-echo",
            label="ECHO",
            title="MIMIC-IV-ECHO (echocardiogram DICOMs and report measurements)",
            releases=(Release("mimic-iv-echo", False, PHYSIONET_CREDENTIALED),),
            marker=(
                "echo-record-list.csv",
                "structured-measurement.csv.gz",
                "structured_measurement.csv.gz",
                "structured-measurement.csv",
                "structured_measurement.csv",
            ),
            parts=(
                Part(
                    "dicom",
                    "one event per DICOM file, linked (echo-record-list.csv + files/)",
                    tables=("echo/records",),
                    link_column="echo_link",
                    raw_source_id_columns=("echo_study_id",),
                ),
                Part(
                    "measurements",
                    "report measurements (structured-measurement.csv.gz); needs no DICOMs",
                    tables=("echo/measurements",),
                    raw_source_id_columns=("echo_measurement_id", "echo_study_id"),
                ),
            ),
            fragment="multimodal/fragments/echo.yaml",
        ),
        Module(
            name="cxr",
            extension="mimic-iv-cxr",
            aliases=("mimic-cxr-jpg",),
            label="CXR",
            title="MIMIC-CXR-JPG (chest radiograph JPGs)",
            releases=(Release("mimic-cxr-jpg", False, PHYSIONET_CREDENTIALED),),
            marker=("mimic-cxr-2.0.0-metadata.csv.gz", "mimic-cxr-2.0.0-metadata.csv"),
            link_column="cxr_link",
            raw_source_id_columns=("cxr_study_id", "cxr_dicom_id"),
            fragment="multimodal/fragments/cxr.yaml",
        ),
        Module(
            name="cxr_db",
            extension="mimic-iv-cxr-db",
            aliases=("mimic-cxr-ext-mimic-cxr-db", "mimic-cxr-db", "cxr-db"),
            label="CXR-DB",
            title="MIMIC-CXR-DB (radiograph studies, CheXpert labels, report and discharge concepts)",
            releases=(Release("mimic-cxr-ext-mimic-cxr-db", False, PHYSIONET_CREDENTIALED),),
            marker=("radiographs.csv", "cxr_db/radiographs.parquet"),
            raw_source_id_columns=("cxr_study_id", "cxr_dicom_id"),
            fragment="extensions/cxr_db.yaml",
            # With MIMIC-CXR-JPG in the build, its image events (same code, same time, plus the
            # link to the JPG) stand for the images; CXR-DB adds the studies, labels and concepts.
            yields_to={"cxr": (("cxr_db/radiographs", "image"),)},
        ),
        Module(
            name="waveform",
            extension="mimic-iv-wdb",
            aliases=("mimic4wdb",),
            label="WDB",
            title="MIMIC-IV Waveform Database (bedside monitor waveforms and numerics, WFDB)",
            releases=(Release("mimic4wdb", False, ODBL),),
            marker=("waves",),
            link_column="waveform_link",
            raw_source_id_columns=("waveform_record_id",),
            fragment="multimodal/fragments/waveform.yaml",
        ),
    )
}

#: Every accepted extension name -> internal module name.
NAMES: dict[str, str] = {n: m.name for m in MODULES.values() for n in m.names}


def parse_selection(spec: str) -> dict[str, tuple[tuple[str, ...], bool]]:
    """`--extensions` value -> `{module: (parts, explicit)}`, in canonical module order.

    An extension is named as `mimic-iv-<x>`, by its PhysioNet slug, or by its short name.

    Examples:
        >>> parse_selection("mimic-iv-echo:measurements, mimic-iv-ed ,note:radiology+discharge")
        {'ed': ((), False), 'note': (('discharge', 'radiology'), True), 'echo': (('measurements',), True)}
        >>> list(parse_selection("mimic-iv-mne,mimic-iv-cxr,mimic4wdb,mimic-cxr-ext-mimic-cxr-db"))
        ['mne', 'cxr', 'cxr_db', 'waveform']
        >>> list(parse_selection("all")) == list(MODULES)
        True
        >>> parse_selection("")
        {}
        >>> parse_selection("mimic-iv-echo:videos")
        Traceback (most recent call last):
            ...
        ValueError: mimic-iv-echo has no part 'videos'; parts: dicom, measurements
    """
    chosen: dict[str, tuple[tuple[str, ...], bool]] = {}
    for item in (x.strip() for x in spec.split(",")):
        if not item:
            continue
        if item == "all":
            chosen |= {m: ((), False) for m in MODULES if m not in chosen}
            continue
        name, _, parts_spec = item.partition(":")
        if name not in NAMES:
            raise ValueError(
                f"unknown extension {name!r}; choose from "
                f"{', '.join(m.names[0] for m in MODULES.values())} (see --list-extensions)"
            )
        module = MODULES[NAMES[name]]
        parts = tuple(p.strip() for p in parts_spec.split("+") if p.strip())
        for p in parts:
            if p not in module.part_names:
                have = ", ".join(module.part_names) or "none (the extension is all-or-nothing)"
                raise ValueError(f"{module.names[0]} has no part {p!r}; parts: {have}")
        if parts:
            parts = tuple(p for p in module.part_names if p in parts)  # canonical order
        chosen[module.name] = (parts, bool(parts))
    return {m: chosen[m] for m in MODULES if m in chosen}


def describe() -> str:
    """The `--list-extensions` text: every extension, its names, parts and releases."""
    lines = []
    for m in (CORE, *MODULES.values()):
        rel = ", ".join(f"{r.project} ({'demo' if r.demo else 'full'}, {r.license})" for r in m.releases)
        name = m.names[0]
        lines.append(f"{name:<16} {m.title}")
        if len(m.names) > 1:
            lines.append(f"{'':<16} also: {', '.join(m.names[1:])}")
        lines.append(f"{'':<16} releases: {rel}")
        if m.link_column:
            lines.append(f"{'':<16} link column: {m.link_column}")
        for p in m.parts:
            link = f" [{p.link_column}]" if p.link_column else ""
            lines.append(f"{'':<16} part {name}:{p.name}: {p.title}{link}")
    lines.append(
        "core (MIMIC-IV) is always built; it defines the cohort every extension is restricted to.\n"
        "A release missing from --root is filled by a simulated one (SYNTHETIC_<slug>.json) when a\n"
        "--root holds one, e.g. this repository's data/synthetic_demo; it is flagged as synthetic."
    )
    return "\n".join(lines)


def _has_marker(d: Path, module: Module) -> bool:
    return (
        d.is_dir()
        and any((d / m).exists() for m in module.markers())
        and all((d / r).exists() for r in module.requires)
    )


def synthetic_info(d: Path, slug: str) -> dict | None:
    """The contents of `d/SYNTHETIC_<slug>.json` (a simulated release), or None."""
    fp = d / f"SYNTHETIC_{slug}.json"
    if not fp.is_file():
        return None
    info = json.loads(fp.read_text())
    return info if info.get("synthetic") else None


def _candidates(root: Path, prefix: str, other_prefixes: tuple[str, ...]) -> list[tuple[Path, str]]:
    """`(dir, version)` pairs under `root` that could hold release `prefix`, newest version last.

    `other_prefixes` are the module's other releases' prefixes that extend this one (the demo
    `mimic-iv-ed-demo` extends `mimic-iv-ed`), so `mimic-iv-ed-demo-2.2/` is not taken for the full
    `mimic-iv-ed` release.

    Examples:
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     root = Path(d)
        ...     for p in ("mimic-iv-ed/2.2", "mimic-iv-ed/2.0", "mimic-iv-ed-demo-2.2", "x-1.0"):
        ...         (root / p).mkdir(parents=True)
        ...     hits = _candidates(root, "mimic-iv-ed", ("mimic-iv-ed-demo",))
        ...     [(c.relative_to(root).as_posix(), v) for c, v in hits]
        [('mimic-iv-ed/2.0', '2.0'), ('mimic-iv-ed/2.2', '2.2')]
        >>> with tempfile.TemporaryDirectory() as d:
        ...     root = Path(d)
        ...     (root / "mimic-iv-ecg-demo-diagnostic-electrocardiogram-matched-subset-demo-0.1").mkdir()
        ...     [v for _, v in _candidates(root, "mimic-iv-ecg-demo", ())]
        ['0.1']
    """
    found: list[tuple[Path, str]] = []
    for base in (root, root / "physionet.org" / "files"):
        project_dir = base / prefix
        if project_dir.is_dir():
            found += [(c, c.name) for c in project_dir.iterdir() if c.is_dir() and _VERSION.fullmatch(c.name)]
        if base.is_dir():
            for c in base.glob(f"{prefix}-*"):
                m = _TRAILING_VERSION.search(c.name)
                if not (c.is_dir() and m):
                    continue
                if any(c.name.startswith(f"{o}-") or c.name.startswith(f"{o}/") for o in other_prefixes):
                    continue
                found.append((c, m.group(1)))
    return sorted(found, key=lambda cv: tuple(int(x) for x in cv[1].split(".")))


def find_release(
    module: Module,
    root: Path | list[Path],
    path: Path | None = None,
    version: str | None = None,
    parts: tuple[str, ...] = (),
    explicit: bool = False,
) -> FoundRelease:
    """Locate `module` under the `root` folder(s) (or at `path`).

    Prefers the full release over the demo when both exist, the newest version, and any real release over a
    simulated one (`SYNTHETIC_<slug>.json`, used only when nothing real is found).
    """
    roots = [root] if isinstance(root, Path) else list(root)
    if path is not None:
        if not _has_marker(path, module):
            want = " or ".join(module.markers()) + "".join(f" and {r}" for r in module.requires)
            raise FileNotFoundError(f"--path {module.names[0]}={path}: no {want} there")
        release = _guess_release(module, path)
        info = synthetic_info(path, release.project)
        if version is None and info:
            version = info.get("dataset_version")
        if version is None:
            m = _TRAILING_VERSION.search(path.name)
            version = m.group(1) if m else (path.name if _VERSION.fullmatch(path.name) else None)
        if version is None:
            raise ValueError(
                f"cannot tell the {module.title} version from {path}; pass --version {module.names[0]}=X"
            )
        return _found(module, release, version, path, parts, explicit, info)

    all_prefixes = [(r, p) for r in module.releases for p in (r.project, *r.aliases)]
    # Full releases first: when someone has both, the build should use the real data.
    for release in sorted(module.releases, key=lambda r: r.demo):
        for prefix in (release.project, *release.aliases):
            longer = tuple(p for _, p in all_prefixes if p != prefix and p.startswith(prefix))
            hits = [
                (c, v)
                for r in roots
                for c, v in _candidates(r, prefix, longer)
                if _has_marker(c, module) and not synthetic_info(c, release.project)
            ]
            if version is not None:
                hits = [(c, v) for c, v in hits if v == version]
            if hits:
                c, v = max(hits, key=lambda cv: _version_key(cv[1]))
                return _found(module, release, v, c, parts, explicit)
    for release in module.releases:
        for r in roots:
            for base in (r, r / "physionet.org" / "files"):
                c = base / release.project
                info = synthetic_info(c, release.project)
                if info and _has_marker(c, module):
                    v = info.get("dataset_version") or "synthetic"
                    if version is None or version == v:
                        return _found(module, release, v, c, parts, explicit, info)
    raise FileNotFoundError(
        f"{module.title} not found under {', '.join(map(str, roots))}: expected one of "
        + ", ".join(f"{r.project}/<version>/ or {r.project}-<version>/" for r in module.releases)
        + f" containing {' or '.join(module.markers())}"
        + "".join(f" and {r}" for r in module.requires)
        + f". Pass --path {module.names[0]}=<dir> to point at it."
    )


def _version_key(v: str) -> tuple[int, ...]:
    return tuple(int(x) for x in _VERSION.match(v).group(0).split(".")) if _VERSION.match(v) else ()


def _found(
    module: Module,
    release: Release,
    version: str,
    root: Path,
    parts: tuple[str, ...],
    explicit: bool,
    synthetic: dict | None = None,
) -> FoundRelease:
    if synthetic is None:
        return FoundRelease(module, release, version, root, parts, explicit)
    extras = {"license": synthetic.get("license", ODBL), "synthetic": synthetic}
    return FoundRelease(module, release, version, root, parts, explicit, extras, synthetic=True)


def _guess_release(module: Module, path: Path) -> Release:
    """The release whose slug (or alias) occurs in `path`, preferring the longest match (demo over full)."""
    text = path.as_posix()
    best = None
    for r in module.releases:
        for p in (r.project, *r.aliases):
            if p in text and (best is None or len(p) > best[0]):
                best = (len(p), r)
    return best[1] if best else module.releases[-1]
