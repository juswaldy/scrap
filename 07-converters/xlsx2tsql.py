
# xlsx_to_tsql.py
"""
CLI tool: Convert the FIRST worksheet of an XLSX file into a T‑SQL CREATE TABLE script.

Usage:
    python xlsx2tsql.py --input path/to/file.xlsx --table-name MyTable --output out.sql

Rules implemented:
  - INT if entire column contains only integers.
  - FLOAT if any decimal appears (in any row) for that column.
  - NVARCHAR(length) if the column contains text; length = max observed text length, capped at 255.
  - Mixed/unclear/missing -> NVARCHAR(255).
  - Requires valid headers: non-empty, unique (case-insensitive) after trimming.
  - Processes only the first worksheet.
"""
import argparse
import math
import sys
from typing import List, Tuple, Optional

import pandas as pd

MAX_VARCHAR = 255

class XlsxToTsqlError(Exception):
    pass

def _read_first_sheet(path: str) -> pd.DataFrame:
    try:
        # dtype=object to avoid premature coercion
        df = pd.read_excel(path, sheet_name=0, dtype=object, engine=None)
    except Exception as e:
        raise XlsxToTsqlError(f"Failed to read XLSX: {e}")
    if df is None or df.empty:
        raise XlsxToTsqlError("The first worksheet is empty or unreadable.")
    return df

def _validate_headers(df: pd.DataFrame) -> List[str]:
    # Ensure columns are not unnamed/empty
    headers = list(df.columns)
    clean = []
    for h in headers:
        # Pandas may label missing headers like 'Unnamed: 0'; treat as invalid
        if h is None:
            raise XlsxToTsqlError("Worksheet contains a missing header (None).")
        hs = str(h).strip()
        if hs == "" or hs.lower().startswith("unnamed:"):
            raise XlsxToTsqlError("Worksheet contains empty or 'Unnamed' header(s).")
        clean.append(hs)

    # Check uniqueness (case-insensitive)
    lowered = [h.lower() for h in clean]
    if len(set(lowered)) != len(lowered):
        raise XlsxToTsqlError("Worksheet contains duplicate headers (case-insensitive)." )

    return clean

def _is_int_like(val) -> bool:
    # True if numeric and integral, or string that parses to int
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return True  # ignore empties when considering "entire column is int"
    if isinstance(val, (int,)):
        return True
    if isinstance(val, float):
        return float(val).is_integer()
    if isinstance(val, str):
        s = val.strip()
        if s == "":
            return True  # blank cells don't break INT rule
        try:
            # allow +/-, no thousands sep
            if "." in s or "e" in s.lower():
                return False
            int(s)
            return True
        except Exception:
            return False
    return False

def _is_float_like(val) -> bool:
    # True if value represents a decimal (non-integer) number
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return False
    if isinstance(val, (int,)):
        return False
    if isinstance(val, float):
        return not float(val).is_integer()
    if isinstance(val, str):
        s = val.strip()
        if s == "":
            return False
        try:
            f = float(s)
            return not float(f).is_integer()
        except Exception:
            return False
    return False

def _is_text(val) -> bool:
    # Consider text if it's a non-empty string that doesn't parse cleanly as a number
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return False
    if isinstance(val, str):
        s = val
        if s.strip() == "":
            return False  # blank is not text
        # If it parses to a number, we won't classify as text here
        try:
            float(s.strip())
            return False
        except Exception:
            return True
    # Non-string mixed types imply textual ambiguity -> treat as text
    if not isinstance(val, (int, float)):
        return True
    return False

def _max_text_len(series: pd.Series) -> int:
    max_len = 0
    for v in series:
        if isinstance(v, str) and v.strip() != "":
            max_len = max(max_len, len(v))
        elif (not isinstance(v, (int, float)) and v is not None and not (isinstance(v, float) and math.isnan(v))):
            # For exotic types (dates/objects) treat as text length via str()
            s = str(v)
            if s.strip() != "":
                max_len = max(max_len, len(s))
    return min(max_len, MAX_VARCHAR)

def _infer_sql_type(series: pd.Series) -> Tuple[str, Optional[int]]:
    saw_float = False
    all_int = True
    saw_text = False

    for v in series:
        if _is_float_like(v):
            saw_float = True
        if not _is_int_like(v):
            all_int = False
        if _is_text(v):
            saw_text = True

    # Decision tree based on rules
    if saw_text and (saw_float or not all_int):
        # Mixed numbers/text or unclear -> NVARCHAR(255)
        return ("NVARCHAR", MAX_VARCHAR)
    if saw_text:
        # Pure text
        length = _max_text_len(series)
        if length <= 0:
            # No measurable text length -> unclear
            return ("NVARCHAR", MAX_VARCHAR)
        return ("NVARCHAR", max(1, length))
    # No text encountered
    if saw_float:
        return ("FLOAT", None)
    if all_int:
        # If the column is entirely empty, treat as unclear text
        non_null_count = series.notna().sum()
        if non_null_count == 0:
            return ("NVARCHAR", MAX_VARCHAR)
        return ("INT", None)
    # Fallback
    return ("NVARCHAR", MAX_VARCHAR)

def _quote_ident(name: str) -> str:
    # Sanitize: wrap with [ ]
    return f"[{name.replace(']', ']]')}]"

def _generate_create(table_name: str, headers: List[str], df: pd.DataFrame) -> str:
    cols_sql = []
    for h in headers:
        col_type, length = _infer_sql_type(df[h])
        if col_type == "NVARCHAR" and length is not None:
            type_sql = f"NVARCHAR({length})"
        else:
            type_sql = col_type
        cols_sql.append(f"    {_quote_ident(h)} {type_sql}")
    cols_block = ",\n".join(cols_sql)
    create_sql = f"CREATE TABLE {_quote_ident(table_name)} (\n{cols_block}\n);"
    return create_sql

def _validate_output(sql: str, headers: List[str]) -> None:
    # Check that every header appears once and has a type
    missing = []
    for h in headers:
        token = _quote_ident(h) + " "
        if token not in sql:
            missing.append(h)
    if missing:
        raise XlsxToTsqlError(f"Validation failed: columns missing from output: {missing}")
    # Rudimentary format check
    if not sql.strip().startswith("CREATE TABLE"):
        raise XlsxToTsqlError("Validation failed: output does not start with 'CREATE TABLE'.")
    if not sql.strip().endswith(");"):
        raise XlsxToTsqlError("Validation failed: output does not end with ');'." )

def convert(input_path: str, table_name: str, output_path: Optional[str]) -> str:
    df = _read_first_sheet(input_path)
    headers = _validate_headers(df)

    # Reindex dataframe to the validated header order to be explicit
    df = df[[h for h in headers]]

    sql = _generate_create(table_name, headers, df)
    _validate_output(sql, headers)

    if output_path is None:
        # Derive .sql alongside input
        if input_path.lower().endswith(".xlsx"):
            output_path = input_path[:-5] + ".sql"
        else:
            output_path = input_path + ".sql"

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(sql + "\n")
    except Exception as e:
        raise XlsxToTsqlError(f"Failed to write SQL file: {e}")

    return sql

def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Generate T‑SQL CREATE TABLE from XLSX first worksheet.")
    parser.add_argument("--input", "-i", required=True, help="Path to the .xlsx file")
    parser.add_argument("--table-name", "-t", default="TableName", help="Name of the SQL table to create")
    parser.add_argument("--output", "-o", help="Path to write the .sql output (optional)")
    args = parser.parse_args(argv)

    try:
        sql = convert(args.input, args.table_name, args.output)
        # Also print to stdout so callers can capture the string
        print(sql)
        return 0
    except XlsxToTsqlError as e:
        sys.stderr.write(f"ERROR: {e}\n")
        return 2
    except Exception as e:
        sys.stderr.write(f"UNEXPECTED ERROR: {e}\n")
        return 3

if __name__ == "__main__":
    raise SystemExit(main())
