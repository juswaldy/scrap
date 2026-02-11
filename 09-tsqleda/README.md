# tsql-eda (CSV/XLSX → SQL Server DDL + EDA Markdown + BULK INSERT)

This is a small Python app that takes a **CSV or XLSX**, infers a reasonable **SQL Server schema**, and generates:

- `CREATE TABLE` (T-SQL)
- `CREATE INDEX` suggestions (T-SQL)
- `BULK INSERT` loading script (T-SQL; CSV-oriented)
- quick EDA notes in **Markdown**

## A note on `UnicodeDecodeError`

Many Windows CSVs are **not UTF-8**; common encodings are `cp1252` / `latin1`.

This project now:
- tries `utf-8`, `utf-8-sig`, then `cp1252`, then `latin1`
- also lets you force an encoding with `--encoding`

## Option A — Streamlit UI

```bash
pip install -r requirements.txt
streamlit run app.py
```

Upload a file and download the generated outputs.

## Option B — CLI

```bash
pip install -r requirements-cli.txt

python tsql_eda.py path/to/data.csv --table dbo.MyTable --outdir out
python tsql_eda.py path/to/data.csv --table dbo.MyTable --encoding cp1252
python tsql_eda.py path/to/data.csv --table dbo.MyTable --bulk-path "\\\\server\\share\\data.csv"
```

Outputs:
- `out/MyTable.create_table.sql`
- `out/MyTable.create_indexes.sql`
- `out/MyTable.bulk_insert.sql` (CSV/TXT inputs)
- `out/MyTable.eda.md`

## BULK INSERT notes

- `BULK INSERT` runs on the **SQL Server instance**, not your laptop.  
  The file must be reachable by the SQL Server service account (server local path or UNC share).
- Basic `FIELDTERMINATOR` loading may not correctly handle **quoted CSV with embedded commas** (depends on SQL Server version).
  If you have complex CSVs, consider:
  - SQL Server 2022+ CSV format options, or
  - SSIS / Azure Data Factory / bcp / staging + parsing.

## Heuristics

- Text defaults to **NVARCHAR** with inferred length (`NVARCHAR(MAX)` if very long).
- Numerics: tries `TINYINT/SMALLINT/INT/BIGINT` by range; otherwise `DECIMAL(p,s)`; else `FLOAT`.
- Datetimes: `DATETIME2(3)` when values parse as datetime.
- PK suggestion: unique, NOT NULL columns; prefers names like `id`, `*_id`, `guid`.
- Index suggestions: unique keys, ID-like columns, and selective datetimes.

Always validate against your domain rules and expected workloads.
