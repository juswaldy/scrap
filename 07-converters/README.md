# 07-converters

Small, standalone Python CLI converters.

## Requirements

- Python 3.8+

Install optional dependencies depending on which tool you use:

```bash
python -m pip install --upgrade pip
python -m pip install openpyxl pandas markdown
```

## Tools

### `csv2xlsx.py` — stream CSV → XLSX

Stream-convert large CSV files (hundreds of MB+) to XLSX using `openpyxl` write-only mode.

Common usage:

```bash
# Convert entire file
python csv2xlsx.py big.csv big.xlsx

# Uniformly sample N rows (reservoir sampling)
python csv2xlsx.py big.csv sample.xlsx --sample 10000 --seed 42

# Split across worksheets if you might exceed Excel's row limit
python csv2xlsx.py big.csv big.xlsx --split-sheets

# Write multiple XLSX chunk files into <input>_chunks/
python csv2xlsx.py big.csv ignored.xlsx --chunk-rows 200000
```

Notes:
- By default the first CSV row is treated as a header; use `--no-header` if not.
- `--chunk-rows` cannot be combined with `--split-sheets`.

### `md2html.py` — batch Markdown → HTML (+ TOC + Mermaid)

Converts all `.md` files in a folder to styled HTML pages and generates a `toc.html`.
Mermaid diagrams are rendered client-side via CDN.

```bash
python md2html.py --input_folder /path/to/mds --output_folder /path/to/out
```

If arguments are omitted, the script prompts interactively.

### `xlsx2tsql.py` — XLSX (first sheet) → T-SQL `CREATE TABLE`

Reads the **first worksheet** of an `.xlsx` file and infers column types to generate a T-SQL `CREATE TABLE` script.

```bash
python xlsx2tsql.py --input path/to/file.xlsx --table-name MyTable --output out.sql
```

Type inference rules:
- `INT` if the entire column contains only integers (ignoring blanks).
- `FLOAT` if any decimal appears.
- `NVARCHAR(n)` for text columns where `n` is the max observed string length (capped at 255).
- Mixed/unclear/missing values fall back to `NVARCHAR(255)`.
- Headers must be non-empty and unique (case-insensitive).
