"""Compose `event_configs.yaml` with each extension fragment into a registered spec.

MEDS-Extract runs one spec file and fails on any configured table whose raw file is missing; it
has no include or optional-table mechanism. An add-on dataset that needs its own PhysioNet
credentials (MIMIC-Note-Extract, and later MIMIC-IV-ED) therefore cannot live in the base spec,
and a hand-kept copy of the base would drift. Instead each add-on is a fragment,
`src/MIMIC_IV_MEDS/extensions/<name>.yaml`, and this script writes
`src/MIMIC_IV_MEDS/event_configs_<name>.yaml` from the base plus the fragment:

- every top-level block of the base except `sources`, verbatim (comments included), with
  `etl.dataset_name` replaced by the fragment's;
- the header comment and every table block of the fragment, verbatim;
- a `sources` block: each bucket the fragment names is the base bucket plus the fragment's
  entries; base buckets the fragment does not name are dropped (the add-on has no data for them,
  so e.g. a `demo` run would fail on its missing tables); `common` is kept; `dataset_version` keeps
  the entries of the remaining buckets. A fragment may restamp a bucket's version with its own
  `dataset_version: {<bucket>: ...}` (the synthetic demos do, so a demo build says it is
  synthetic); the base entries of that bucket then get the base version written in literally, so
  their download URLs still name the base release.

The output is committed (the entry point needs a file in the package) and
`tests/extension_specs_test.py` fails when it is stale. After editing the base or a fragment:

    uv run python scripts/build_extension_specs.py           # rewrite the generated specs
    uv run python scripts/build_extension_specs.py --check   # exit 1 if any is stale
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import yaml

PKG = Path(__file__).resolve().parent.parent / "src" / "MIMIC_IV_MEDS"
BASE = PKG / "event_configs.yaml"
EXTENSIONS = PKG / "extensions"

_TOP_KEY = re.compile(r"^([A-Za-z_][^:#\s]*):")
_DATASET_NAME = re.compile(r"^(  dataset_name: )(\S+)[ \t]*$", re.MULTILINE)


class _PrettierStyleDumper(yaml.SafeDumper):
    """Dump YAML the way the repo's prettier hook formats it.

    Prettier indents block sequences under their key and prefers double quotes; matching it keeps the hook
    from rewriting the generated file, which would then fail the freshness check.
    """

    def increase_indent(self, flow: bool = False, indentless: bool = False):
        return super().increase_indent(flow, False)

    def choose_scalar_style(self):
        style = super().choose_scalar_style()
        return '"' if style == "'" else style


def split_top_level(text: str) -> list[tuple[str | None, str]]:
    """Split YAML text into `(key, chunk)` pairs, one per top-level key, in order.

    Column-0 comment lines and blank lines directly above a key belong to that key's chunk; text
    after the last key's body is its own `(None, ...)` chunk.

    Examples:
        >>> split_top_level("# head\\na:\\n  x: 1\\n\\n# about b\\nb: 2\\n")
        [('a', '# head\\na:\\n  x: 1\\n'), ('b', '\\n# about b\\nb: 2\\n')]
        >>> split_top_level("a: 1\\n# trailing\\n")
        [('a', 'a: 1\\n'), (None, '# trailing\\n')]
    """
    chunks: list[tuple[str | None, str]] = []
    key: str | None = None
    body: list[str] = []
    pending: list[str] = []  # column-0 comments / blanks not yet known to be inside a block
    for line in text.splitlines(keepends=True):
        m = _TOP_KEY.match(line)
        if m:
            if key is not None:
                chunks.append((key, "".join(body)))
            key, body, pending = m.group(1), [*pending, line], []
        elif line.strip() == "" or line.startswith("#"):
            pending.append(line)
        else:
            body.extend(pending)
            pending = []
            body.append(line)
    if key is not None:
        chunks.append((key, "".join(body)))
    if pending:
        chunks.append((None, "".join(pending)))
    return chunks


def merge_sources(base: dict, ext: dict) -> dict:
    """Merge a fragment's `sources` into the base's (see the module docstring).

    Examples:
        >>> base = {"dataset_version": {"dataset": "3.1", "demo": "2.2"},
        ...         "dataset": [{"u": "mimic"}], "demo": [{"u": "demo"}], "common": [{"u": "map"}]}
        >>> merged = merge_sources(base, {"dataset": [{"u": "ext"}]})
        >>> merged["dataset_version"], merged["dataset"], merged["common"]
        ({'dataset': '3.1'}, [{'u': 'mimic'}, {'u': 'ext'}], [{'u': 'map'}])
        >>> list(merged)  # `demo` is gone: the fragment has no data for it
        ['dataset_version', 'dataset', 'common']

        A fragment's own `dataset_version` restamps its buckets; the base entries keep the base
        release in their URLs:

        >>> base["demo"] = [{"u": "demo/${sources.dataset_version.demo}"}]
        >>> merged = merge_sources(base, {"dataset_version": {"demo": "2.2+x-0.1"}, "demo": [{"u": "x"}]})
        >>> merged["dataset_version"], merged["demo"]
        ({'demo': '2.2+x-0.1'}, [{'u': 'demo/2.2'}, {'u': 'x'}])
    """
    reserved = {"dataset_version", "common"}
    merged: dict = {}
    kept = [b for b in base if b not in reserved and b in ext]
    missing = sorted(b for b in ext if b not in reserved and b not in base)
    if missing:
        raise ValueError(f"fragment names buckets the base does not have: {missing}")
    version = base.get("dataset_version")
    restamp = ext.get("dataset_version") or {}
    if not isinstance(restamp, dict) or set(restamp) - set(kept):
        raise ValueError(f"a fragment's dataset_version must map buckets it names to versions; got {restamp}")
    if restamp and not isinstance(version, dict):
        raise ValueError("a fragment can only restamp buckets of a per-bucket base dataset_version")
    if version is not None:
        merged["dataset_version"] = (
            {b: restamp.get(b, v) for b, v in version.items() if b in kept}
            if isinstance(version, dict)
            else version
        )
    for bucket in kept:
        base_entries = base[bucket]
        if bucket in restamp:
            base_entries = _pin_version(
                base_entries, f"${{sources.dataset_version.{bucket}}}", version[bucket]
            )
        merged[bucket] = [*base_entries, *ext[bucket]]
    if "common" in base or "common" in ext:
        merged["common"] = [*base.get("common", []), *ext.get("common", [])]
    return merged


def _pin_version(node, interpolation: str, value: str):
    """Replace `interpolation` with `value` in every string of a nested list/dict.

    Examples:
        >>> _pin_version([{"u": "a/${v}/b", "n": [1, "${v}"]}], "${v}", "2.2")
        [{'u': 'a/2.2/b', 'n': [1, '2.2']}]
    """
    if isinstance(node, str):
        return node.replace(interpolation, value)
    if isinstance(node, list):
        return [_pin_version(x, interpolation, value) for x in node]
    if isinstance(node, dict):
        return {k: _pin_version(v, interpolation, value) for k, v in node.items()}
    return node


def compose(base_text: str, ext_text: str, ext_path: Path) -> str:
    base_chunks = split_top_level(base_text)
    ext_chunks = split_top_level(ext_text)
    ext_doc = yaml.safe_load(ext_text)
    base_doc = yaml.safe_load(base_text)

    name = ext_doc.get("etl", {}).get("dataset_name")
    if not name:
        raise ValueError(f"{ext_path} must set etl.dataset_name")
    if "sources" not in ext_doc:
        raise ValueError(f"{ext_path} must declare a sources: block")
    clash = sorted(set(ext_doc) & set(base_doc) - {"etl", "sources"})
    if clash:
        raise ValueError(f"{ext_path} redefines base tables: {clash}")

    out = [
        "# GENERATED by scripts/build_extension_specs.py from event_configs.yaml and\n"
        f"# extensions/{ext_path.name}. This is the `{name}` spec. Do not edit it: change those two files\n"
        "# and re-run the script.\n\n"
    ]
    for key, chunk in base_chunks:
        if key == "sources":
            sources_comment = chunk[: chunk.index("sources:")]
            continue
        if key == "etl":
            chunk, n = _DATASET_NAME.subn(rf"\g<1>{name}", chunk)
            if n != 1:
                raise ValueError("expected exactly one `  dataset_name:` line in the base etl block")
        out.append(chunk)
    for key, chunk in ext_chunks:
        if key == "etl":
            # The fragment's header comment sits above its `etl:` block; keep the comment, since
            # the etl block itself was merged into the base's above.
            out.append("\n" + chunk[: chunk.index("etl:")].strip("\n") + "\n")
        elif key != "sources":
            out.append(chunk if chunk.startswith("\n") else "\n" + chunk)
    merged = merge_sources(base_doc["sources"], ext_doc["sources"])
    out.append(
        "\n"
        + sources_comment.lstrip("\n")
        + f"#\n# Composed from the base `sources:` and extensions/{ext_path.name} by the build script.\n"
        + yaml.dump(
            {"sources": merged},
            Dumper=_PrettierStyleDumper,
            sort_keys=False,
            default_flow_style=False,
            width=1000,
        )
    )
    return "".join(out)


def targets() -> dict[Path, str]:
    base_text = BASE.read_text()
    return {
        PKG / f"event_configs_{frag.stem}.yaml": compose(base_text, frag.read_text(), frag)
        for frag in sorted(EXTENSIONS.glob("*.yaml"))
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--check", action="store_true", help="exit 1 if a generated spec is stale")
    args = parser.parse_args(argv)
    stale = []
    for path, text in targets().items():
        if path.exists() and path.read_text() == text:
            continue
        stale.append(path)
        if not args.check:
            path.write_text(text)
            print(f"wrote {path.relative_to(PKG.parent.parent)}")
    if args.check and stale:
        print("stale generated specs (run scripts/build_extension_specs.py):", *stale, sep="\n  ")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
