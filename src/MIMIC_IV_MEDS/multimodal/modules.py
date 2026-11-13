"""The modules a multimodal build can combine, and how their PhysioNet releases are found on disk.

A *module* is one PhysioNet project family (MIMIC-IV-ED, MIMIC-IV-ECG, ...). Each has one or more
*releases* (the open demo, the credentialed full release), told apart by project slug. A release is
found under the build's `--root` in either of PhysioNet's two layouts:

- `<root>/<slug>/<version>/` (and `<root>/physionet.org/files/<slug>/<version>/`): what
  `wget -r` or the "files" browser give;
- `<root>/<slug>[-<title words>]-<version>/`: what the "Download the ZIP file" button gives,
  e.g. `mimic-iv-clinical-database-demo-2.2/` or
  `mimic-iv-ecg-demo-diagnostic-electrocardiogram-matched-subset-demo-0.1/`.

A candidate directory only counts when it holds the release's marker file, so an unrelated folder
with a matching name is ignored. `--path <module>=<dir>` (and `--version`) bypass the search.
"""

from __future__ import annotations

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
        if self.explicit and set(self.parts) != set(self.module.part_names):
            return f"{self.module.label}({'+'.join(self.parts)})"
        return self.module.label

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
            label="ED",
            title="MIMIC-IV-ED",
            releases=(
                Release("mimic-iv-ed-demo", True, ODBL),
                Release("mimic-iv-ed", False, PHYSIONET_CREDENTIALED),
            ),
            marker="ed/edstays.csv.gz",
            raw_source_id_columns=("ed_stay_id",),
        ),
        Module(
            name="note",
            label="Note",
            title="MIMIC-IV-Note (discharge summaries and radiology reports)",
            releases=(Release("mimic-iv-note", False, PHYSIONET_CREDENTIALED),),
            marker=("note/discharge.csv.gz", "note/radiology.csv.gz"),
            raw_source_id_columns=("note_id",),
            parts=(
                Part("discharge", "discharge summaries and their addenda (note/discharge.csv.gz)"),
                Part("radiology", "radiology reports and their addenda (note/radiology.csv.gz)"),
            ),
        ),
        Module(
            name="ecg",
            label="ECG",
            title="MIMIC-IV-ECG (diagnostic 12-lead ECGs, WFDB)",
            releases=(
                Release("mimic-iv-ecg-demo", True, ODBL),
                Release("mimic-iv-ecg", False, ODBL),
            ),
            marker="record_list.csv",
            link_column="ecg_link",
            raw_source_id_columns=("ecg_study_id",),
        ),
        Module(
            name="echo",
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
        ),
        Module(
            name="cxr",
            label="CXR",
            title="MIMIC-CXR-JPG (chest radiograph JPGs)",
            releases=(Release("mimic-cxr-jpg", False, PHYSIONET_CREDENTIALED),),
            marker=("mimic-cxr-2.0.0-metadata.csv.gz", "mimic-cxr-2.0.0-metadata.csv"),
            link_column="cxr_link",
            raw_source_id_columns=("cxr_study_id", "cxr_dicom_id"),
        ),
        Module(
            name="waveform",
            label="WDB",
            title="MIMIC-IV Waveform Database (bedside monitor waveforms and numerics, WFDB)",
            releases=(Release("mimic4wdb", False, ODBL),),
            marker=("waves",),
            link_column="waveform_link",
            raw_source_id_columns=("waveform_record_id",),
        ),
    )
}


def parse_selection(spec: str) -> dict[str, tuple[tuple[str, ...], bool]]:
    """`--modules` value -> `{module: (parts, explicit)}`, in canonical module order.

    Examples:
        >>> parse_selection("echo:measurements, ed ,note:radiology+discharge")
        {'ed': ((), False), 'note': (('discharge', 'radiology'), True), 'echo': (('measurements',), True)}
        >>> list(parse_selection("all")) == list(MODULES)
        True
        >>> parse_selection("")
        {}
        >>> parse_selection("echo:videos")
        Traceback (most recent call last):
            ...
        ValueError: echo has no part 'videos'; parts: dicom, measurements
    """
    chosen: dict[str, tuple[tuple[str, ...], bool]] = {}
    for item in (x.strip() for x in spec.split(",")):
        if not item:
            continue
        if item == "all":
            chosen |= {m: ((), False) for m in MODULES if m not in chosen}
            continue
        name, _, parts_spec = item.partition(":")
        if name not in MODULES:
            raise ValueError(f"unknown module {name!r}; choose from {', '.join(MODULES)}")
        module = MODULES[name]
        parts = tuple(p.strip() for p in parts_spec.split("+") if p.strip())
        for p in parts:
            if p not in module.part_names:
                have = ", ".join(module.part_names) or "none (the module is all-or-nothing)"
                raise ValueError(f"{name} has no part {p!r}; parts: {have}")
        if parts:
            parts = tuple(p for p in module.part_names if p in parts)  # canonical order
        chosen[name] = (parts, bool(parts))
    return {m: chosen[m] for m in MODULES if m in chosen}


def describe() -> str:
    """The `--list-modules` text: every module, its parts and the releases it is found as."""
    lines = []
    for m in (CORE, *MODULES.values()):
        rel = ", ".join(f"{r.project} ({'demo' if r.demo else 'full'}, {r.license})" for r in m.releases)
        lines.append(f"{m.name:<9} {m.title}\n{'':<9} releases: {rel}")
        if m.link_column:
            lines.append(f"{'':<9} link column: {m.link_column}")
        for p in m.parts:
            link = f" [{p.link_column}]" if p.link_column else ""
            lines.append(f"{'':<9} part {m.name}:{p.name}: {p.title}{link}")
    lines.append("core is always built; it defines the cohort every other module is restricted to.")
    return "\n".join(lines)


def _has_marker(d: Path, module: Module) -> bool:
    return d.is_dir() and any((d / m).exists() for m in module.markers())


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
    root: Path,
    path: Path | None = None,
    version: str | None = None,
    parts: tuple[str, ...] = (),
    explicit: bool = False,
) -> FoundRelease:
    """Locate `module` under `root` (or at `path`); prefers the full release over the demo when both exist."""
    if path is not None:
        if not _has_marker(path, module):
            raise FileNotFoundError(f"--path {module.name}={path}: no {' or '.join(module.markers())} there")
        release = _guess_release(module, path)
        if version is None:
            m = _TRAILING_VERSION.search(path.name)
            version = m.group(1) if m else (path.name if _VERSION.fullmatch(path.name) else None)
        if version is None:
            raise ValueError(
                f"cannot tell the {module.title} version from {path}; pass --version {module.name}=X"
            )
        return FoundRelease(module, release, version, path, parts, explicit)

    all_prefixes = [(r, p) for r in module.releases for p in (r.project, *r.aliases)]
    # Full releases first: when someone has both, the build should use the real data.
    for release in sorted(module.releases, key=lambda r: r.demo):
        for prefix in (release.project, *release.aliases):
            longer = tuple(p for _, p in all_prefixes if p != prefix and p.startswith(prefix))
            hits = [(c, v) for c, v in _candidates(root, prefix, longer) if _has_marker(c, module)]
            if version is not None:
                hits = [(c, v) for c, v in hits if v == version]
            if hits:
                c, v = hits[-1]
                return FoundRelease(module, release, v, c, parts, explicit)
    raise FileNotFoundError(
        f"{module.title} not found under {root}: expected one of "
        + ", ".join(f"{r.project}/<version>/ or {r.project}-<version>/" for r in module.releases)
        + f" containing {' or '.join(module.markers())}. Pass --path {module.name}=<dir> to point at it."
    )


def _guess_release(module: Module, path: Path) -> Release:
    """The release whose slug (or alias) occurs in `path`, preferring the longest match (demo over full)."""
    text = path.as_posix()
    best = None
    for r in module.releases:
        for p in (r.project, *r.aliases):
            if p in text and (best is None or len(p) > best[0]):
                best = (len(p), r)
    return best[1] if best else module.releases[-1]
