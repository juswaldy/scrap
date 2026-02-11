"""
tsql_eda.py
CLI to read CSV/XLSX and generate:
- CREATE TABLE
- CREATE INDEX suggestions
- BULK INSERT script (CSV only, for SQL Server)
- EDA notes (Markdown)

Examples:
  python tsql_eda.py data.csv --table dbo.MyTable --outdir out
  python tsql_eda.py data.csv --table dbo.MyTable --encoding cp1252
  python tsql_eda.py data.csv --table dbo.MyTable --bulk-path "\\\\server\\share\\data.csv"
"""
from __future__ import annotations

import argparse
import pathlib

import pandas as pd

from generator import (
    profile_dataframe,
    generate_create_table_sql,
    generate_create_indexes_sql,
    generate_bulk_insert_sql,
    generate_eda_markdown,
)
from io_utils import read_csv_best_effort, read_excel_best_effort, detect_row_terminator_from_path


def parse_table(table: str):
    if "." in table:
        schema, name = table.split(".", 1)
    else:
        schema, name = "dbo", table
    return schema, name


def read_input(path: str, sheet: str | None = None, *, encoding: str | None = None, sep: str | None = None):
    p = pathlib.Path(path)
    if not p.exists():
        raise FileNotFoundError(path)

    ext = p.suffix.lower()
    if ext in {".csv", ".txt"}:
        df, used_enc = read_csv_best_effort(path, encoding=encoding, sep=sep or ",")
        return df, used_enc
    if ext in {".xlsx", ".xls"}:
        df = read_excel_best_effort(path, sheet_name=sheet or 0)
        return df, None
    raise ValueError(f"Unsupported file extension: {ext}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("input", help="Path to CSV/XLSX")
    ap.add_argument("--table", required=True, help="Target table name, e.g. dbo.MyTable")
    ap.add_argument("--sheet", default=None, help="Excel sheet name (XLSX only)")
    ap.add_argument("--outdir", default="out", help="Output directory (default: out)")
    ap.add_argument("--pk", default=None, help="Override primary key column name")
    ap.add_argument("--surrogate-pk", action="store_true", help="Add BIGINT IDENTITY surrogate PK")
    ap.add_argument("--max-scan", type=int, default=200000, help="Max non-null values to scan per column")

    # CSV parsing options
    ap.add_argument("--encoding", default=None, help="CSV encoding to try first (e.g. utf-8, cp1252). If omitted, uses fallbacks.")
    ap.add_argument("--sep", default=",", help="CSV delimiter (default: ,)")
    ap.add_argument("--header", action="store_true", help="CSV has header row (default: true).")
    ap.add_argument("--no-header", action="store_true", help="CSV has NO header row (FIRSTROW=1 for bulk insert).")

    # BULK INSERT options
    ap.add_argument("--bulk-path", default=None, help="File path as seen by SQL Server (e.g. C:\\data\\file.csv or \\\\server\\share\\file.csv).")
    ap.add_argument("--bulk-firstrow", type=int, default=None, help="Override FIRSTROW for BULK INSERT. If omitted, uses 2 when header exists else 1.")
    ap.add_argument("--bulk-fieldterm", default=None, help="Override FIELDTERMINATOR (default: sep)")
    ap.add_argument("--bulk-rowterm", default=None, help="Override ROWTERMINATOR (default auto-detect 0x0d0a vs 0x0a)")
    ap.add_argument("--bulk-codepage", type=int, default=None, help="Override CODEPAGE (e.g. 65001 for UTF-8, 1252 for cp1252).")

    args = ap.parse_args(argv)

    schema, table = parse_table(args.table)
    df, used_encoding = read_input(args.input, args.sheet, encoding=args.encoding, sep=args.sep)

    tp = profile_dataframe(df, max_scan_per_column=args.max_scan)

    pk = args.pk or tp.chosen_pk
    add_surrogate = bool(args.surrogate_pk) or (pk is None)

    effective_pk = f"{table}_sk" if add_surrogate else pk

    create_table = generate_create_table_sql(tp, schema, table, pk_column=pk, add_surrogate_pk=add_surrogate)
    create_indexes = generate_create_indexes_sql(tp, schema, table, pk_column=effective_pk)
    eda_md = generate_eda_markdown(df, tp, schema, table, pk_column=effective_pk)

    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    (outdir / f"{table}.create_table.sql").write_text(create_table, encoding="utf-8")
    (outdir / f"{table}.create_indexes.sql").write_text(create_indexes, encoding="utf-8")
    (outdir / f"{table}.eda.md").write_text(eda_md, encoding="utf-8")

    # BULK INSERT script: only meaningful for CSV/TXT inputs
    ext = pathlib.Path(args.input).suffix.lower()
    if ext in {".csv", ".txt"}:
        bulk_path = args.bulk_path or "<PATH_VISIBLE_TO_SQL_SERVER>"

        # FIRSTROW logic
        has_header = True
        if args.no_header:
            has_header = False
        if args.header:
            has_header = True

        firstrow = args.bulk_firstrow if args.bulk_firstrow is not None else (2 if has_header else 1)
        fieldterm = args.bulk_fieldterm or args.sep
        rowterm = args.bulk_rowterm or detect_row_terminator_from_path(args.input)

        # CODEPAGE logic (best-effort)
        codepage = args.bulk_codepage
        if codepage is None and used_encoding:
            enc = used_encoding.lower().replace("-", "")
            if enc in {"utf8", "utf8sig"}:
                codepage = 65001
            elif enc in {"cp1252", "windows1252"}:
                codepage = 1252
            elif enc in {"latin1", "iso88591"}:
                # latin1 isn't a SQL Server codepage name, but 1252 is usually closest on Windows.
                codepage = 1252

        bulk_sql = generate_bulk_insert_sql(
            schema,
            table,
            bulk_path,
            first_row=firstrow,
            field_terminator=fieldterm,
            row_terminator=rowterm,
            codepage=codepage,
        )
        (outdir / f"{table}.bulk_insert.sql").write_text(bulk_sql, encoding="utf-8")

    print(f"Wrote:")
    print(f"  {outdir / f'{table}.create_table.sql'}")
    print(f"  {outdir / f'{table}.create_indexes.sql'}")
    print(f"  {outdir / f'{table}.eda.md'}")
    if ext in {'.csv', '.txt'}:
        print(f"  {outdir / f'{table}.bulk_insert.sql'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
