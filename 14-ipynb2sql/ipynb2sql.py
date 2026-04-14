#!/usr/bin/env python3
"""
Convert Jupyter/Azure Data Studio/VS Code SQL notebooks (.ipynb) into .sql files.

Why this exists
---------------
Azure Data Studio notebooks and the MSSQL extension in VS Code use native
Jupyter notebooks (.ipynb). Those notebooks are great interactively, but they
are painful to diff, archive, review in pull requests, or run in non-notebook
SQL tooling. This script exports the executable SQL in notebook order while
preserving Markdown and mixed-kernel context as SQL comments.

Key features
------------
- Zero third-party dependencies (stdlib only)
- Supports Azure Data Studio / VS Code SQL notebook metadata patterns
- Preserves Markdown as SQL comments
- Handles mixed notebooks (SQL + Python/PowerShell/etc.)
- Strips common SQL magics such as %%sql, %sql, #!sql, #!sql-mydb
- Optional commented snapshots of cell outputs
- Works on single files or entire directory trees
- Writes atomic output files to avoid partial writes on failure

Examples
--------
Export one notebook next to the source file:
    python ipynb2sql.py report.ipynb

Export one notebook to stdout:
    python ipynb2sql.py report.ipynb --stdout

Export a folder recursively into build/sql:
    python ipynb2sql.py notebooks/ --recursive --output build/sql

Treat non-SQL code cells as errors instead of commenting them out:
    python ipynb2sql.py notebook.ipynb --non-sql error

Use comment-only cell separation instead of GO batches:
    python ipynb2sql.py notebook.ipynb --cell-separator comment
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence


SQLISH_LANGUAGES = {
    "sql",
    "tsql",
    "t-sql",
    "mssql",
    "mysql",
    "postgresql",
    "postgres",
    "sqlite",
    "plsql",
    "snowflake",
    "bigquery",
    "redshift",
    "duckdb",
    "trino",
    "presto",
    "oracle",
    "db2",
    "sparksql",
}

SQLISH_PREFIXES = (
    "sql-",
    "sql_",
    "mssql-",
    "mssql_",
    "tsql-",
    "tsql_",
    "t-sql-",
    "t-sql_",
)

SQL_MAGIC_RE = re.compile(
    r"^\s*(?P<magic>%%sql|%%tsql|%%mssql|%sql|%tsql|%mssql|#!sql(?:[-_][^\s]+)?|#!tsql(?:[-_][^\s]+)?|#!mssql(?:[-_][^\s]+)?)\b(?P<args>.*)$",
    re.IGNORECASE,
)

CONNECT_MAGIC_RE = re.compile(r"^\s*#!connect\s+(?:mssql|sql|sqlserver)\b(?P<args>.*)$", re.IGNORECASE)

SQL_FIRST_TOKEN_RE = re.compile(
    r"^(?:;\s*)?(select|with|insert|update|delete|merge|create|alter|drop|truncate|exec(?:ute)?|declare|use|begin|if|while|set|grant|revoke|deny|backup|restore|dbcc|commit|rollback)\b",
    re.IGNORECASE,
)

LIKELY_NON_SQL_RE = re.compile(
    r"^(from|import|def|class|lambda|print\s*\(|for\b|async\b|await\b|function\b|console\.log\s*\(|Write-Host\b|\$[A-Za-z_][\w:]*\s*=)",
    re.IGNORECASE,
)

HEADING_RE = re.compile(r"^\s{0,3}#\s+(.+?)\s*$")


class ConversionError(Exception):
    """Raised when a notebook cannot be converted."""


@dataclass(slots=True)
class Options:
    include_markdown: bool = True
    include_raw: bool = False
    include_outputs: bool = False
    markdown_style: str = "block"  # block | line | off
    non_sql: str = "comment"  # comment | ignore | error
    cell_separator: str = "go"  # go | comment | blank
    annotate_cells: bool = True
    strip_sql_magics: bool = True
    infer_sql: bool = True
    include_header: bool = True
    max_output_lines: int = 30


@dataclass(slots=True)
class Stats:
    total_cells: int = 0
    sql_cells: int = 0
    markdown_cells: int = 0
    raw_cells: int = 0
    commented_non_sql_cells: int = 0
    ignored_non_sql_cells: int = 0
    output_blocks: int = 0


@dataclass(slots=True)
class RenderedChunk:
    text: str
    executable_sql: bool


@dataclass(slots=True)
class ConversionResult:
    sql_text: str
    stats: Stats


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def eprint(*args: object) -> None:
    print(*args, file=sys.stderr)



def deep_get(mapping: Any, *path: str) -> Any:
    cur = mapping
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur



def normalize_source(source: Any) -> str:
    if source is None:
        return ""
    if isinstance(source, str):
        return source
    if isinstance(source, list):
        return "".join(str(part) for part in source)
    return str(source)



def normalize_language(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    return text



def is_sql_language(language: str | None) -> bool:
    if not language:
        return False
    lang = language.strip().lower()
    if lang in SQLISH_LANGUAGES:
        return True
    if lang.startswith(SQLISH_PREFIXES):
        return True
    if lang == "sql-notebook":
        return True
    return False



def escape_block_comment(text: str) -> str:
    return text.replace("*/", "* /")



def line_comment(text: str, prefix: str = "-- ") -> str:
    if not text:
        return prefix.rstrip()
    lines = text.splitlines()
    if not lines:
        return prefix.rstrip()
    return "\n".join(prefix + line if line else prefix.rstrip() for line in lines)



def block_comment(title: str, body: str | None = None) -> str:
    title = escape_block_comment(title.rstrip())
    parts = [f"/* {title}"]
    if body:
        parts.append("")
        parts.append(escape_block_comment(body.rstrip()))
    parts.append("*/")
    return "\n".join(parts)



def first_markdown_heading(cells: Sequence[dict[str, Any]]) -> str | None:
    for cell in cells:
        if cell.get("cell_type") != "markdown":
            continue
        text = normalize_source(cell.get("source"))
        for line in text.splitlines():
            match = HEADING_RE.match(line)
            if match:
                return match.group(1).strip()
    return None



def strip_leading_sql_magic(source: str) -> tuple[str, str | None]:
    """
    Remove a leading SQL-ish magic or polyglot kernel selector.

    Returns (clean_source, note).
    """
    if not source.strip():
        return source, None

    lines = source.splitlines(keepends=True)
    first_content_index = None
    for i, line in enumerate(lines):
        if line.strip():
            first_content_index = i
            break
    if first_content_index is None:
        return source, None

    first_line = lines[first_content_index]
    magic_match = SQL_MAGIC_RE.match(first_line)
    if magic_match:
        magic = magic_match.group("magic")
        args = magic_match.group("args").strip()
        if magic.startswith("%%") or magic.startswith("#!"):
            new_lines = lines[:first_content_index] + lines[first_content_index + 1 :]
            note = f"stripped leading {magic}"
            if args:
                note += f" ({args})"
            return "".join(new_lines), note
        if magic.startswith("%"):
            replacement = args + ("\n" if first_line.endswith("\n") else "")
            new_lines = lines[:]
            new_lines[first_content_index] = replacement
            note = f"stripped leading {magic}"
            return "".join(new_lines), note

    connect_match = CONNECT_MAGIC_RE.match(first_line)
    if connect_match:
        args = connect_match.group("args").strip()
        new_lines = lines[:first_content_index] + lines[first_content_index + 1 :]
        note = "stripped leading #!connect SQL directive"
        if args:
            note += f" ({args})"
        return "".join(new_lines), note

    return source, None



def remove_leading_comments(text: str) -> str:
    # Repeatedly strip leading whitespace and SQL comment blocks / line comments.
    remaining = text
    while True:
        previous = remaining
        remaining = re.sub(r"^\s+", "", remaining, flags=re.DOTALL)
        remaining = re.sub(r"^--[^\n]*(?:\n|$)", "", remaining)
        remaining = re.sub(r"^/\*.*?\*/\s*", "", remaining, flags=re.DOTALL)
        if remaining == previous:
            break
    return remaining



def looks_like_sql(source: str) -> bool:
    text = source.strip()
    if not text:
        return False

    first_nonblank = next((line.strip() for line in source.splitlines() if line.strip()), "")
    if SQL_MAGIC_RE.match(first_nonblank) or CONNECT_MAGIC_RE.match(first_nonblank):
        return True

    cleaned = remove_leading_comments(text)
    if not cleaned:
        return False

    if SQL_FIRST_TOKEN_RE.match(cleaned):
        return True

    if re.search(r"(?im)^\s*go\s*$", cleaned):
        return True

    if re.search(r"\bselect\b", cleaned, flags=re.IGNORECASE) and re.search(r"\bfrom\b", cleaned, flags=re.IGNORECASE):
        return True

    first_line = cleaned.splitlines()[0].strip()
    if LIKELY_NON_SQL_RE.match(first_line):
        return False

    return False



def render_markdown(index: int, text: str, style: str, title: str = "Markdown") -> str:
    header = f"Cell {index}: {title}"
    body = text.rstrip()
    if style == "off":
        return ""
    if style == "line":
        content = f"[{header}]"
        if body:
            content += "\n\n" + body
        return line_comment(content)
    return block_comment(header, body)



def render_non_sql_code(index: int, language: str | None, source: str, note: str | None = None) -> str:
    label = language or "non-sql"
    title = f"Cell {index}: {label} code (commented out)"
    if note:
        title += f" | {note}"
    return block_comment(title, source.rstrip())



def ends_with_batch_separator(text: str, separator: str = "GO") -> bool:
    lines = [line.strip() for line in text.rstrip().splitlines()]
    for line in reversed(lines):
        if line:
            return line.upper() == separator.upper()
    return False



def render_outputs(outputs: list[dict[str, Any]], max_lines: int) -> str | None:
    if not outputs:
        return None

    rendered_blocks: list[str] = []
    for output in outputs:
        otype = output.get("output_type")
        if otype == "stream":
            text = normalize_source(output.get("text"))
            if text.strip():
                rendered_blocks.append(text.rstrip())
            continue
        if otype == "error":
            traceback = normalize_source(output.get("traceback"))
            ename = output.get("ename") or "Error"
            evalue = output.get("evalue") or ""
            text = traceback.strip() or f"{ename}: {evalue}".strip()
            if text:
                rendered_blocks.append(text)
            continue
        if otype in {"display_data", "execute_result"}:
            data = output.get("data") if isinstance(output.get("data"), dict) else {}
            if "text/plain" in data:
                rendered_blocks.append(normalize_source(data["text/plain"]).rstrip())
                continue
            if "application/json" in data:
                rendered_blocks.append(json.dumps(data["application/json"], ensure_ascii=False, indent=2))
                continue
            if "application/vnd.dataresource+json" in data:
                rendered_blocks.append(json.dumps(data["application/vnd.dataresource+json"], ensure_ascii=False, indent=2))
                continue
            if data:
                mime_names = ", ".join(sorted(data.keys()))
                rendered_blocks.append(f"[non-text output: {mime_names}]")
            continue

    if not rendered_blocks:
        return None

    text = "\n\n".join(block.strip() for block in rendered_blocks if block.strip())
    lines = text.splitlines()
    truncated = False
    if len(lines) > max_lines:
        lines = lines[:max_lines]
        truncated = True
    body = "\n".join(lines)
    if truncated:
        body += "\n\n[output truncated]"
    return block_comment("Cell output snapshot", body)


# ---------------------------------------------------------------------------
# Metadata detection
# ---------------------------------------------------------------------------


def extract_notebook_language(nb: dict[str, Any]) -> str | None:
    metadata = nb.get("metadata") if isinstance(nb.get("metadata"), dict) else {}

    language_info_candidates = [
        deep_get(metadata, "custom", "metadata", "language_info"),
        deep_get(metadata, "metadata", "language_info"),
        deep_get(metadata, "language_info"),
    ]
    for language_info in language_info_candidates:
        if not isinstance(language_info, dict):
            continue
        lang = normalize_language(language_info.get("name"))
        if lang:
            return lang

    kernelspec_candidates = [
        deep_get(metadata, "custom", "metadata", "kernelspec"),
        deep_get(metadata, "metadata", "kernelspec"),
        deep_get(metadata, "kernelspec"),
    ]
    for kernelspec in kernelspec_candidates:
        if not isinstance(kernelspec, dict):
            continue
        for key in ("language", "name", "display_name"):
            lang = normalize_language(kernelspec.get(key))
            if lang:
                return lang

    return None



def extract_cell_language(cell: dict[str, Any]) -> str | None:
    metadata = cell.get("metadata") if isinstance(cell.get("metadata"), dict) else {}

    candidates = [
        deep_get(metadata, "language"),
        deep_get(metadata, "vscode", "languageId"),
        deep_get(metadata, "dotnet_interactive", "language"),
        deep_get(metadata, "polyglot_notebook", "kernelName"),
        deep_get(metadata, "custom", "metadata", "language"),
        deep_get(metadata, "custom", "metadata", "vscode", "languageId"),
    ]
    for candidate in candidates:
        lang = normalize_language(candidate)
        if lang:
            return lang
    return None


# ---------------------------------------------------------------------------
# Conversion core
# ---------------------------------------------------------------------------


def convert_notebook(nb: dict[str, Any], source_name: str, options: Options) -> ConversionResult:
    if not isinstance(nb, dict):
        raise ConversionError("Notebook root must be a JSON object.")
    if not isinstance(nb.get("cells"), list):
        raise ConversionError("Notebook JSON does not contain a valid 'cells' array.")

    cells: list[dict[str, Any]] = nb["cells"]
    notebook_language = extract_notebook_language(nb)
    stats = Stats(total_cells=len(cells))
    chunks: list[RenderedChunk] = []

    title = first_markdown_heading(cells)

    for idx, cell in enumerate(cells, start=1):
        ctype = cell.get("cell_type")
        source = normalize_source(cell.get("source"))
        outputs = cell.get("outputs") if isinstance(cell.get("outputs"), list) else []

        if ctype == "markdown":
            stats.markdown_cells += 1
            if options.include_markdown and options.markdown_style != "off":
                attachments = cell.get("attachments")
                body = source.rstrip()
                if isinstance(attachments, dict) and attachments:
                    attachment_names = ", ".join(sorted(str(name) for name in attachments))
                    note = f"Attachments: {attachment_names}"
                    body = f"{body}\n\n{note}" if body else note
                text = render_markdown(idx, body, options.markdown_style)
                if text:
                    chunks.append(RenderedChunk(text=text, executable_sql=False))
            continue

        if ctype == "raw":
            stats.raw_cells += 1
            if options.include_raw:
                text = render_markdown(idx, source, options.markdown_style, title="Raw")
                if text:
                    chunks.append(RenderedChunk(text=text, executable_sql=False))
            continue

        if ctype != "code":
            chunks.append(
                RenderedChunk(
                    text=block_comment(f"Cell {idx}: unsupported cell type '{ctype}'", source.rstrip()),
                    executable_sql=False,
                )
            )
            continue

        explicit_cell_language = extract_cell_language(cell)
        cleaned_source = source
        magic_note = None
        if options.strip_sql_magics:
            cleaned_source, magic_note = strip_leading_sql_magic(cleaned_source)

        explicit_non_sql = explicit_cell_language is not None and not is_sql_language(explicit_cell_language)
        explicit_sql = explicit_cell_language is not None and is_sql_language(explicit_cell_language)
        notebook_is_sql = is_sql_language(notebook_language)
        inferred_sql = options.infer_sql and looks_like_sql(source)
        sql_by_magic = options.strip_sql_magics and magic_note is not None

        is_sql_cell = explicit_sql or sql_by_magic or (not explicit_non_sql and notebook_is_sql) or (
            not explicit_non_sql and inferred_sql
        )

        if is_sql_cell:
            sql_text = cleaned_source.rstrip()
            if not sql_text:
                # Empty SQL-ish cell: keep only a comment when there was a stripped magic.
                if magic_note and options.annotate_cells:
                    chunks.append(
                        RenderedChunk(
                            text=block_comment(f"Cell {idx}: empty SQL cell | {magic_note}"),
                            executable_sql=False,
                        )
                    )
                continue

            stats.sql_cells += 1
            parts: list[str] = []
            if options.annotate_cells:
                header = f"Cell {idx}: SQL"
                if explicit_cell_language and explicit_cell_language != "sql":
                    header += f" | language={explicit_cell_language}"
                elif notebook_language and notebook_language != "sql" and notebook_is_sql:
                    header += f" | notebook language={notebook_language}"
                if magic_note:
                    header += f" | {magic_note}"
                parts.append(block_comment(header))
            parts.append(sql_text)
            if options.include_outputs:
                rendered_output = render_outputs(outputs, options.max_output_lines)
                if rendered_output:
                    parts.append(rendered_output)
                    stats.output_blocks += 1
            chunks.append(RenderedChunk(text="\n\n".join(p for p in parts if p), executable_sql=True))
            continue

        # Non-SQL code cell.
        if options.non_sql == "ignore":
            stats.ignored_non_sql_cells += 1
            continue
        if options.non_sql == "error":
            lang_text = explicit_cell_language or notebook_language or "unknown"
            raise ConversionError(
                f"Cell {idx} is non-SQL code (detected language: {lang_text}). "
                f"Use --non-sql comment or --non-sql ignore to continue."
            )

        stats.commented_non_sql_cells += 1
        note = magic_note
        text = render_non_sql_code(idx, explicit_cell_language or notebook_language, source.rstrip(), note=note)
        chunks.append(RenderedChunk(text=text, executable_sql=False))

    body = join_chunks(chunks, options.cell_separator)

    if options.include_header:
        header_lines = []
        if title:
            header_lines.append(f"Title: {title}")
        header_lines.extend(
            [
                f"Source notebook: {source_name}",
                f"Notebook language: {notebook_language or 'unknown'}",
                f"Generated at (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')}",
                (
                    "Summary: "
                    f"{stats.sql_cells} SQL cell(s), "
                    f"{stats.markdown_cells} Markdown cell(s), "
                    f"{stats.raw_cells} raw cell(s), "
                    f"{stats.commented_non_sql_cells} commented non-SQL code cell(s), "
                    f"{stats.ignored_non_sql_cells} ignored non-SQL code cell(s)"
                ),
            ]
        )
        header = block_comment("Notebook export", "\n".join(header_lines))
        body = header if not body else f"{header}\n\n{body}"

    if body and not body.endswith("\n"):
        body += "\n"

    return ConversionResult(sql_text=body, stats=stats)



def join_chunks(chunks: Sequence[RenderedChunk], cell_separator: str) -> str:
    if not chunks:
        return ""

    sql_indices = [i for i, chunk in enumerate(chunks) if chunk.executable_sql]
    last_sql_index = sql_indices[-1] if sql_indices else None

    out: list[str] = []
    for i, chunk in enumerate(chunks):
        out.append(chunk.text.rstrip())
        if chunk.executable_sql and last_sql_index is not None and i != last_sql_index:
            if cell_separator == "go" and not ends_with_batch_separator(chunk.text):
                out.append("GO")
            elif cell_separator == "comment":
                out.append(block_comment(f"End of SQL cell"))
        out.append("")

    return "\n".join(piece for piece in out if piece is not None).rstrip()


# ---------------------------------------------------------------------------
# IO helpers
# ---------------------------------------------------------------------------


def load_notebook(path: Path) -> dict[str, Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        text = path.read_text(encoding="utf-8-sig")
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ConversionError(f"Invalid JSON in notebook '{path}': {exc}") from exc
    return data



def atomic_write(path: Path, text: str, force: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not force:
        raise ConversionError(f"Output file already exists: {path} (use --force to overwrite)")

    fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            try:
                os.remove(tmp_name)
            except OSError:
                pass



def resolve_output_path(
    input_path: Path,
    root_input: Path,
    output: Path | None,
    multiple_sources: bool,
) -> Path:
    default_name = input_path.with_suffix(".sql").name

    if output is None:
        return input_path.with_suffix(".sql")

    if multiple_sources or output.is_dir() or str(output).endswith((os.sep, "/")):
        relative = input_path.relative_to(root_input) if root_input.is_dir() else Path(input_path.name)
        return output / relative.with_suffix(".sql")

    return output



def gather_notebooks(inputs: Sequence[str], recursive: bool) -> list[tuple[Path, Path]]:
    gathered: list[tuple[Path, Path]] = []
    seen: set[Path] = set()

    for item in inputs:
        path = Path(item).expanduser().resolve()
        if not path.exists():
            raise ConversionError(f"Input path does not exist: {path}")

        if path.is_file():
            if path.suffix.lower() != ".ipynb":
                raise ConversionError(f"Input file is not an .ipynb notebook: {path}")
            if path not in seen:
                gathered.append((path, path.parent))
                seen.add(path)
            continue

        pattern_iter: Iterable[Path]
        if recursive:
            pattern_iter = path.rglob("*.ipynb")
        else:
            pattern_iter = path.glob("*.ipynb")
        for notebook in sorted(pattern_iter):
            resolved = notebook.resolve()
            if resolved not in seen:
                gathered.append((resolved, path.resolve()))
                seen.add(resolved)

    if not gathered:
        raise ConversionError("No .ipynb notebooks found.")

    return gathered


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert .ipynb notebooks to .sql with Azure Data Studio / VS Code SQL notebook awareness."
    )
    parser.add_argument("inputs", nargs="+", help="Notebook file(s) or directory/directories containing .ipynb files")
    parser.add_argument(
        "-o",
        "--output",
        help=(
            "Output file or directory. For one input file, this can be a file path. "
            "For multiple inputs or directory inputs, this must be a directory."
        ),
    )
    parser.add_argument("--stdout", action="store_true", help="Write a single converted notebook to stdout")
    parser.add_argument("-r", "--recursive", action="store_true", help="Recursively search input directories for notebooks")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output files")
    parser.add_argument(
        "--markdown-style",
        choices=("block", "line", "off"),
        default="block",
        help="How Markdown cells should be preserved in SQL output",
    )
    parser.add_argument(
        "--non-sql",
        choices=("comment", "ignore", "error"),
        default="comment",
        help="How to handle non-SQL code cells in mixed notebooks",
    )
    parser.add_argument(
        "--cell-separator",
        choices=("go", "comment", "blank"),
        default="go",
        help="How to separate executable SQL cells in the exported script",
    )
    parser.add_argument(
        "--include-outputs",
        action="store_true",
        help="Include a commented snapshot of text-based cell outputs when present",
    )
    parser.add_argument(
        "--max-output-lines",
        type=int,
        default=30,
        help="Maximum lines of output to preserve per cell when --include-outputs is used",
    )
    parser.add_argument(
        "--no-markdown",
        action="store_true",
        help="Do not include Markdown cells in the SQL output",
    )
    parser.add_argument("--include-raw", action="store_true", help="Include raw cells as SQL comments")
    parser.add_argument(
        "--no-annotate-cells",
        action="store_true",
        help="Do not insert cell header comments before exported SQL cells",
    )
    parser.add_argument(
        "--no-strip-magics",
        action="store_true",
        help="Do not strip leading SQL magics such as %%sql or #!sql",
    )
    parser.add_argument(
        "--no-infer-sql",
        action="store_true",
        help="Do not use SQL heuristics for ambiguous cells; rely only on metadata/magics/notebook kernel",
    )
    parser.add_argument(
        "--no-header",
        action="store_true",
        help="Do not include a generated export header at the top of the file",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress progress messages on stderr")
    return parser



def options_from_args(args: argparse.Namespace) -> Options:
    return Options(
        include_markdown=not args.no_markdown,
        include_raw=args.include_raw,
        include_outputs=args.include_outputs,
        markdown_style=args.markdown_style,
        non_sql=args.non_sql,
        cell_separator=args.cell_separator,
        annotate_cells=not args.no_annotate_cells,
        strip_sql_magics=not args.no_strip_magics,
        infer_sql=not args.no_infer_sql,
        include_header=not args.no_header,
        max_output_lines=max(1, int(args.max_output_lines)),
    )



def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        notebooks = gather_notebooks(args.inputs, recursive=args.recursive)
        if args.stdout:
            if len(notebooks) != 1:
                raise ConversionError("--stdout requires exactly one input notebook.")
            if args.output:
                raise ConversionError("--stdout cannot be used together with --output.")

        output_path = Path(args.output).expanduser().resolve() if args.output else None
        if output_path and len(notebooks) > 1 and output_path.exists() and output_path.is_file():
            raise ConversionError("When converting multiple notebooks, --output must be a directory.")

        options = options_from_args(args)

        for input_path, root_input in notebooks:
            nb = load_notebook(input_path)
            result = convert_notebook(nb, source_name=str(input_path), options=options)

            if args.stdout:
                sys.stdout.write(result.sql_text)
                continue

            destination = resolve_output_path(
                input_path=input_path,
                root_input=root_input,
                output=output_path,
                multiple_sources=len(notebooks) > 1 or any(Path(p).is_dir() for p in args.inputs),
            )
            atomic_write(destination, result.sql_text, force=args.force)
            if not args.quiet:
                eprint(
                    f"Converted {input_path} -> {destination} "
                    f"[{result.stats.sql_cells} SQL cell(s), {result.stats.markdown_cells} Markdown cell(s), "
                    f"{result.stats.commented_non_sql_cells} commented non-SQL code cell(s)]"
                )
        return 0
    except ConversionError as exc:
        eprint(f"Error: {exc}")
        return 2
    except KeyboardInterrupt:
        eprint("Cancelled.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
