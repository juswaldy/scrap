"""Merge a tab-indented tree of names with an HTML/XML <ul><li> tree of aliases.

Both inputs must describe the same tree (same node count, same DFS order).
The output preserves the indentation of the names file and appends the alias
to each line, separated by ``--separator`` (default ``;``).

Usage:
    python mergenodes.py NAMES.txt ALIASES.html [-o OUT.txt] [-s ';']

Aliases are extracted from any element whose text is the visible label of a
tree node. By default we look for the pattern used by PrimeFaces tree
components: ``<span class="...ui-treenode-label..."><span ...>LABEL</span></span>``.
Pass ``--label-class`` to override the marker class.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


LABEL_RE_TEMPLATE = (
    r'class="[^"]*\b{cls}\b[^"]*"[^>]*>\s*'  # outer label span opening tag
    r'<span[^>]*>(?P<text>.*?)</span>'        # inner span with the visible text
)


def extract_aliases(html: str, label_class: str) -> list[str]:
    pattern = re.compile(LABEL_RE_TEMPLATE.format(cls=re.escape(label_class)), re.DOTALL)
    aliases = []
    for m in pattern.finditer(html):
        text = m.group("text")
        # strip nested tags, collapse whitespace
        text = re.sub(r"<[^>]+>", "", text)
        text = re.sub(r"\s+", " ", text).strip()
        aliases.append(text)
    return aliases


def parse_names(text: str) -> list[tuple[str, str]]:
    """Return list of (indent, name) preserving the original leading whitespace."""
    rows = []
    for raw in text.splitlines():
        if not raw.strip():
            continue
        m = re.match(r"^(\s*)(.*\S)\s*$", raw)
        if not m:
            continue
        rows.append((m.group(1), m.group(2)))
    return rows


def merge(names_path: Path, html_path: Path, separator: str, label_class: str) -> str:
    names = parse_names(names_path.read_text(encoding="utf-8"))
    aliases = extract_aliases(html_path.read_text(encoding="utf-8"), label_class)

    if len(names) != len(aliases):
        raise SystemExit(
            f"Node count mismatch: {len(names)} names vs {len(aliases)} aliases.\n"
            f"First few names:   {[n for _, n in names[:5]]}\n"
            f"First few aliases: {aliases[:5]}"
        )

    out_lines = [f"{indent}{name}{separator}{alias}" for (indent, name), alias in zip(names, aliases)]
    return "\n".join(out_lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("names", type=Path, help="Tab-indented names file")
    p.add_argument("aliases", type=Path, help="HTML/XML aliases file (ul/li tree)")
    p.add_argument("-o", "--output", type=Path, help="Output file (default: stdout)")
    p.add_argument("-s", "--separator", default=";", help="Separator between name and alias (default ';')")
    p.add_argument(
        "--label-class",
        default="ui-treenode-label",
        help="CSS class on the element wrapping each node's visible label (default ui-treenode-label)",
    )
    args = p.parse_args(argv)

    merged = merge(args.names, args.aliases, args.separator, args.label_class)

    if args.output:
        args.output.write_text(merged, encoding="utf-8")
    else:
        sys.stdout.write(merged)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
