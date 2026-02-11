"""
generator.py
Core logic for:
- inferring SQL Server column types from a pandas DataFrame
- generating T-SQL CREATE TABLE + CREATE INDEX scripts
- generating BULK INSERT script
- generating EDA notes in Markdown
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd


SQL_IDENTIFIER_MAX = 128
INDEX_KEY_BYTES_LIMIT = 900  # SQL Server nonclustered index key limit (bytes)


# ---------------------------
# Helpers
# ---------------------------

def bracket(name: str) -> str:
    """Bracket-quote an identifier for SQL Server, escaping closing brackets."""
    return f"[{str(name).replace(']', ']]')}]"


def escape_tsql_string_literal(s: str) -> str:
    """Escape a value to be safe inside single-quoted T-SQL string literals."""
    return str(s).replace("'", "''")


def normalize_name_for_object(name: str) -> str:
    """Convert arbitrary text to something safe-ish for SQL object names."""
    s = re.sub(r"[^A-Za-z0-9_]+", "_", str(name).strip())
    s = re.sub(r"__+", "_", s).strip("_")
    return s or "obj"


def truncate_identifier(name: str, max_len: int = SQL_IDENTIFIER_MAX) -> str:
    return name[:max_len]


def unique_name(base: str, used: set) -> str:
    """Ensure uniqueness by appending _2, _3, ..."""
    if base not in used:
        used.add(base)
        return base
    i = 2
    while True:
        cand = truncate_identifier(f"{base}_{i}")
        if cand not in used:
            used.add(cand)
            return cand
        i += 1


def is_uuid_like(s: str) -> bool:
    return bool(re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", s))


# ---------------------------
# Profiling dataclasses
# ---------------------------

@dataclass
class ColumnProfile:
    name: str
    pandas_dtype: str
    inferred_sql_type: str
    nullable: bool

    n_missing: int
    missing_pct: float
    n_unique: int
    unique_pct: float

    sample_values: List[str] = field(default_factory=list)

    # For strings
    max_len: Optional[int] = None

    # For numeric/datetime
    min_value: Optional[str] = None
    max_value: Optional[str] = None

    warnings: List[str] = field(default_factory=list)

    # Simple heuristic flags
    is_candidate_pk: bool = False
    is_unique: bool = False


@dataclass
class TableProfile:
    n_rows: int
    n_cols: int
    column_profiles: List[ColumnProfile]
    pk_candidates: List[str]
    chosen_pk: Optional[str]
    add_surrogate_pk: bool
    notes: List[str] = field(default_factory=list)


# ---------------------------
# Type inference
# ---------------------------

_INT_TYPES: List[Tuple[str, int, int]] = [
    ("TINYINT", 0, 255),
    ("SMALLINT", -32768, 32767),
    ("INT", -2147483648, 2147483647),
    ("BIGINT", -9223372036854775808, 9223372036854775807),
]


def _infer_int_type(min_v: int, max_v: int) -> str:
    for typ, lo, hi in _INT_TYPES:
        if min_v >= lo and max_v <= hi:
            return typ
    return "DECIMAL(38,0)"


def _decimal_precision_scale_from_decimal(d: Decimal) -> Tuple[int, int]:
    t = d.as_tuple()
    digits = len(t.digits)
    exp = t.exponent
    if exp >= 0:
        return digits + exp, 0
    scale = -exp
    return digits, scale


def _infer_decimal_from_values(values: Sequence[Any]) -> Optional[Tuple[int, int]]:
    max_p = 0
    max_s = 0
    seen = 0
    for v in values:
        if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
            continue
        try:
            d = Decimal(str(v))
        except (InvalidOperation, ValueError):
            return None
        p, s = _decimal_precision_scale_from_decimal(d.normalize())
        max_p = max(max_p, p)
        max_s = max(max_s, s)
        seen += 1
        if max_p > 38:
            return None
    if seen == 0:
        return None
    max_p = max(max_p, max_s)
    if max_p > 38:
        return None
    return max_p, max_s


def _try_parse_datetime(series: pd.Series, sample_n: int = 5000) -> float:
    non_null = series.dropna()
    if non_null.empty:
        return 0.0
    if len(non_null) > sample_n:
        non_null = non_null.sample(sample_n, random_state=42)
    parsed = pd.to_datetime(non_null, errors="coerce", infer_datetime_format=True, utc=False)
    return float(parsed.notna().mean())


def _try_parse_numeric(series: pd.Series, sample_n: int = 20000) -> Tuple[float, pd.Series]:
    non_null = series.dropna()
    if non_null.empty:
        return 0.0, pd.Series([], dtype="float64")
    if len(non_null) > sample_n:
        non_null = non_null.sample(sample_n, random_state=42)
    parsed = pd.to_numeric(non_null, errors="coerce")
    return float(parsed.notna().mean()), parsed


def infer_sql_type(series: pd.Series, max_scan: int = 200000) -> Tuple[str, Dict[str, Any]]:
    info: Dict[str, Any] = {"warnings": []}
    s = series

    non_null = s.dropna()
    if len(non_null) > max_scan:
        non_null = non_null.sample(max_scan, random_state=42)
        info["warnings"].append(f"Type inference scanned a sample of {max_scan:,} non-null values (column is large).")

    if non_null.empty:
        return "NVARCHAR(255)", info

    if pd.api.types.is_bool_dtype(s):
        return "BIT", info

    if pd.api.types.is_datetime64_any_dtype(s):
        return "DATETIME2(3)", info

    if pd.api.types.is_integer_dtype(s):
        try:
            return _infer_int_type(int(non_null.min()), int(non_null.max())), info
        except Exception:
            info["warnings"].append("Integer range inference failed; using BIGINT.")
            return "BIGINT", info

    if pd.api.types.is_float_dtype(s):
        vals = non_null.astype(float).values
        if np.all(np.isfinite(vals)):
            frac = np.abs(vals - np.round(vals))
            if float(np.nanmax(frac)) < 1e-9:
                return _infer_int_type(int(np.nanmin(vals)), int(np.nanmax(vals))), info

        dec = _infer_decimal_from_values(non_null.tolist())
        if dec is not None:
            p, sc = dec
            p = max(p, 1)
            return f"DECIMAL({p},{sc})", info
        info["warnings"].append("Decimal inference exceeded limits; using FLOAT.")
        return "FLOAT", info

    # object/string
    sample = non_null
    if len(sample) > 5000:
        sample = sample.sample(5000, random_state=42)
    sample_str = sample.astype(str)

    uuid_ratio = float(sample_str.map(is_uuid_like).mean())
    if uuid_ratio >= 0.95:
        return "UNIQUEIDENTIFIER", info

    bool_tokens = {"true", "false", "t", "f", "y", "n", "yes", "no", "0", "1"}
    bool_ratio = float(sample_str.str.strip().str.lower().isin(bool_tokens).mean())
    if bool_ratio >= 0.99:
        return "BIT", info

    dt_ratio = _try_parse_datetime(non_null)
    if dt_ratio >= 0.95:
        return "DATETIME2(3)", info

    num_ratio, parsed = _try_parse_numeric(non_null)
    if num_ratio >= 0.99:
        parsed = parsed.dropna()
        if parsed.empty:
            return "FLOAT", info
        frac = np.abs(parsed.values - np.round(parsed.values))
        if float(np.nanmax(frac)) < 1e-9:
            return _infer_int_type(int(np.nanmin(parsed.values)), int(np.nanmax(parsed.values))), info
        dec = _infer_decimal_from_values(parsed.tolist())
        if dec is not None:
            p, sc = dec
            p = max(p, 1)
            return f"DECIMAL({p},{sc})", info
        return "FLOAT", info

    max_len = int(sample_str.map(len).max())
    if max_len <= 0:
        max_len = 1
    if max_len <= 4000:
        return f"NVARCHAR({max_len})", info
    return "NVARCHAR(MAX)", info


# ---------------------------
# PK & index suggestions
# ---------------------------

def suggest_primary_key(profiles: List[ColumnProfile]) -> List[str]:
    candidates: List[ColumnProfile] = []
    for p in profiles:
        t = p.inferred_sql_type.upper()
        if p.nullable:
            continue
        if not p.is_unique:
            continue
        if t == "FLOAT":
            continue
        if t.startswith("DECIMAL(") and ",0)" not in t:
            continue
        if t.startswith("NVARCHAR("):
            m = re.search(r"NVARCHAR\((\d+)\)", t)
            if m and int(m.group(1)) > 200:
                continue
        candidates.append(p)

    def score(p: ColumnProfile) -> int:
        name = p.name.strip().lower()
        s = 0
        if name == "id":
            s += 100
        if name.endswith("_id") or (name.endswith("id") and name != "id"):
            s += 60
        if "guid" in name or "uuid" in name:
            s += 50
        t = p.inferred_sql_type.upper()
        if t in {"TINYINT", "SMALLINT", "INT", "BIGINT"}:
            s += 20
        if t == "UNIQUEIDENTIFIER":
            s += 15
        s += int(10 * p.unique_pct)
        return s

    candidates_sorted = sorted(candidates, key=score, reverse=True)
    return [c.name for c in candidates_sorted]


def should_index_column(p: ColumnProfile, pk_name: Optional[str]) -> Tuple[bool, str]:
    if pk_name and p.name == pk_name:
        return False, "Primary key (will be indexed)."

    t = p.inferred_sql_type.upper()

    if "MAX" in t and ("VARCHAR" in t or "NVARCHAR" in t):
        return False, "MAX text column."

    if t == "FLOAT":
        return False, "FLOAT columns are poor index keys."

    if t.startswith("NVARCHAR("):
        m = re.search(r"NVARCHAR\((\d+)\)", t)
        if m and int(m.group(1)) * 2 > INDEX_KEY_BYTES_LIMIT:
            return False, f"Index key would exceed {INDEX_KEY_BYTES_LIMIT} bytes."

    name = p.name.strip().lower()
    if p.is_unique and not p.nullable:
        return True, "Unique and NOT NULL."

    if name.endswith("_id") or (name.endswith("id") and name != "id"):
        if p.unique_pct >= 0.01:
            return True, "Looks like an ID column."

    if t.startswith("DATETIME2") and p.unique_pct >= 0.01:
        return True, "Datetime with useful selectivity."

    if p.unique_pct < 0.005:
        return False, "Low selectivity."

    return False, "No strong index signal."


# ---------------------------
# Main profiling + generation
# ---------------------------

def profile_dataframe(
    df: pd.DataFrame,
    max_scan_per_column: int = 200000,
    sample_values_n: int = 5,
) -> TableProfile:
    n_rows, n_cols = df.shape
    profiles: List[ColumnProfile] = []

    for col in df.columns:
        s = df[col]
        n_missing = int(s.isna().sum())
        missing_pct = float(n_missing / n_rows) if n_rows else 0.0
        n_unique = int(s.nunique(dropna=True))
        unique_pct = float(n_unique / max(n_rows - n_missing, 1))

        sql_type, info = infer_sql_type(s, max_scan=max_scan_per_column)
        nullable = n_missing > 0

        sample = s.dropna()
        if not sample.empty:
            if len(sample) > 5000:
                sample = sample.sample(5000, random_state=42)
            try:
                top = sample.value_counts().head(sample_values_n).index.tolist()
            except Exception:
                top = sample.head(sample_values_n).tolist()
            sample_values = [str(v)[:80] for v in top]
        else:
            sample_values = []

        p = ColumnProfile(
            name=str(col),
            pandas_dtype=str(s.dtype),
            inferred_sql_type=sql_type,
            nullable=nullable,
            n_missing=n_missing,
            missing_pct=missing_pct,
            n_unique=n_unique,
            unique_pct=unique_pct,
            sample_values=sample_values,
            warnings=list(info.get("warnings", [])),
        )

        if sql_type.upper().startswith(("NVARCHAR", "VARCHAR", "NCHAR", "CHAR")):
            non_null = s.dropna().astype(str)
            if len(non_null) > max_scan_per_column:
                non_null = non_null.sample(max_scan_per_column, random_state=42)
            if not non_null.empty:
                p.max_len = int(non_null.map(len).max())

        if pd.api.types.is_numeric_dtype(s) or sql_type.upper().startswith(("INT", "BIGINT", "SMALLINT", "TINYINT", "DECIMAL", "FLOAT")):
            try:
                non_null_num = pd.to_numeric(s.dropna(), errors="coerce").dropna()
                if not non_null_num.empty:
                    p.min_value = str(non_null_num.min())
                    p.max_value = str(non_null_num.max())
            except Exception:
                pass

        if sql_type.upper().startswith("DATETIME2"):
            try:
                non_null_dt = pd.to_datetime(s.dropna(), errors="coerce").dropna()
                if not non_null_dt.empty:
                    p.min_value = str(non_null_dt.min())
                    p.max_value = str(non_null_dt.max())
            except Exception:
                pass

        p.is_unique = (not p.nullable) and (p.n_unique == (n_rows - p.n_missing)) and (n_rows > 0)
        profiles.append(p)

    pk_candidates = suggest_primary_key(profiles)
    chosen_pk = pk_candidates[0] if pk_candidates else None
    add_surrogate = chosen_pk is None

    for p in profiles:
        if p.name in pk_candidates:
            p.is_candidate_pk = True

    tp = TableProfile(
        n_rows=n_rows,
        n_cols=n_cols,
        column_profiles=profiles,
        pk_candidates=pk_candidates,
        chosen_pk=chosen_pk,
        add_surrogate_pk=add_surrogate,
        notes=[],
    )

    try:
        dup_rows = int(df.duplicated().sum())
        if dup_rows:
            tp.notes.append(f"{dup_rows:,} duplicate rows detected (exact duplicates across all columns).")
    except Exception:
        pass

    return tp


def generate_create_table_sql(
    table_profile: TableProfile,
    schema_name: str,
    table_name: str,
    pk_column: Optional[str] = None,
    add_surrogate_pk: bool = False,
) -> str:
    schema_name = schema_name or "dbo"
    pk_column = pk_column or table_profile.chosen_pk
    add_surrogate_pk = bool(add_surrogate_pk)

    lines: List[str] = []
    full_name = f"{bracket(schema_name)}.{bracket(table_name)}"

    lines.append(f"-- Auto-generated by tsql-eda")
    lines.append(f"-- Rows observed: {table_profile.n_rows:,}")
    lines.append("")
    lines.append(f"IF OBJECT_ID(N'{escape_tsql_string_literal(schema_name)}.{escape_tsql_string_literal(table_name)}', N'U') IS NOT NULL")
    lines.append(f"    DROP TABLE {full_name};")
    lines.append("GO")
    lines.append("")

    col_lines: List[str] = []
    constraints: List[str] = []

    if add_surrogate_pk:
        sk = f"{table_name}_sk"
        col_lines.append(f"    {bracket(sk)} BIGINT IDENTITY(1,1) NOT NULL,")

        # Keep pk_column as the business PK for uniqueness decisions elsewhere, but
        # the actual constraint is on the surrogate key.
        pk_column = sk

    for p in table_profile.column_profiles:
        nullness = "NULL" if p.nullable else "NOT NULL"
        col_lines.append(f"    {bracket(p.name)} {p.inferred_sql_type} {nullness},")

    if pk_column:
        pk_name = truncate_identifier(f"PK_{normalize_name_for_object(table_name)}")
        constraints.append(f"    CONSTRAINT {bracket(pk_name)} PRIMARY KEY CLUSTERED ({bracket(pk_column)})")

    lines.append(f"CREATE TABLE {full_name} (")
    if constraints:
        if col_lines:
            col_lines[-1] = col_lines[-1].rstrip(",")
        lines.extend(col_lines)
        lines.append(",")
        lines.extend(constraints)
    else:
        if col_lines:
            col_lines[-1] = col_lines[-1].rstrip(",")
        lines.extend(col_lines)
    lines.append(");")
    lines.append("GO")
    lines.append("")
    return "\n".join(lines)


def generate_create_indexes_sql(
    table_profile: TableProfile,
    schema_name: str,
    table_name: str,
    pk_column: Optional[str] = None,
) -> str:
    schema_name = schema_name or "dbo"
    pk_column = pk_column or table_profile.chosen_pk
    full_name = f"{bracket(schema_name)}.{bracket(table_name)}"

    used_names: set = set()
    stmts: List[str] = []
    stmts.append(f"-- Auto-generated index suggestions by tsql-eda")
    stmts.append("")

    for p in table_profile.column_profiles:
        should, reason = should_index_column(p, pk_column)
        if not should:
            continue

        base = "UQ" if (p.is_unique and not p.nullable) else "IX"
        idx_base = f"{base}_{normalize_name_for_object(table_name)}_{normalize_name_for_object(p.name)}"
        idx_name = unique_name(truncate_identifier(idx_base), used_names)

        unique_kw = "UNIQUE " if (p.is_unique and not p.nullable) else ""
        stmt = f"CREATE {unique_kw}NONCLUSTERED INDEX {bracket(idx_name)} ON {full_name} ({bracket(p.name)});"
        stmts.append(f"-- {reason}")
        stmts.append(stmt)
        stmts.append("GO")
        stmts.append("")

    if len(stmts) == 2:
        stmts.append("-- (No indexes were suggested by the heuristics.)")
        stmts.append("")
    return "\n".join(stmts)


def generate_bulk_insert_sql(
    schema_name: str,
    table_name: str,
    data_file_path_for_server: str,
    *,
    first_row: int = 2,
    field_terminator: str = ",",
    row_terminator: str = "0x0d0a",
    codepage: Optional[int] = None,
    tablock: bool = True,
    keepnulls: bool = False,
    check_constraints: bool = False,
    fire_triggers: bool = False,
    batchsize: Optional[int] = None,
    maxerrors: Optional[int] = None,
    errorfile: Optional[str] = None,
) -> str:
    """
    Generate a BULK INSERT script for SQL Server.

    IMPORTANT: the path is evaluated on the SQL Server machine.
    """
    schema_name = schema_name or "dbo"
    full_name = f"{bracket(schema_name)}.{bracket(table_name)}"
    file_lit = escape_tsql_string_literal(data_file_path_for_server)

    opts: List[str] = []
    opts.append(f"FIRSTROW = {int(first_row)}")
    opts.append(f"FIELDTERMINATOR = '{escape_tsql_string_literal(field_terminator)}'")
    # ROWTERMINATOR can be hex like 0x0d0a or literal '\n' but hex is less ambiguous.
    if row_terminator.lower().startswith("0x"):
        opts.append(f"ROWTERMINATOR = '{row_terminator.lower()}'")
    else:
        opts.append(f"ROWTERMINATOR = '{escape_tsql_string_literal(row_terminator)}'")

    if codepage is not None:
        opts.append(f"CODEPAGE = '{int(codepage)}'")

    if tablock:
        opts.append("TABLOCK")
    if keepnulls:
        opts.append("KEEPNULLS")
    if check_constraints:
        opts.append("CHECK_CONSTRAINTS")
    if fire_triggers:
        opts.append("FIRE_TRIGGERS")
    if batchsize is not None:
        opts.append(f"BATCHSIZE = {int(batchsize)}")
    if maxerrors is not None:
        opts.append(f"MAXERRORS = {int(maxerrors)}")
    if errorfile is not None:
        opts.append(f"ERRORFILE = '{escape_tsql_string_literal(errorfile)}'")

    lines: List[str] = []
    lines.append("-- Auto-generated BULK INSERT by tsql-eda")
    lines.append("-- NOTE: BULK INSERT runs on the SQL Server instance.")
    lines.append("--       The file path must be accessible to the SQL Server service account")
    lines.append("--       (local disk on the server or a UNC share like \\\\server\\share\\file.csv).")
    lines.append("")
    lines.append(f"BULK INSERT {full_name}")
    lines.append(f"FROM '{file_lit}'")
    lines.append("WITH (")
    lines.append("    " + ",\n    ".join(opts))
    lines.append(");")
    lines.append("GO")
    lines.append("")
    return "\n".join(lines)


def _md_escape(s: str) -> str:
    return str(s).replace("|", r"\|").replace("\n", " ").strip()


def generate_eda_markdown(
    df: pd.DataFrame,
    table_profile: TableProfile,
    schema_name: str,
    table_name: str,
    pk_column: Optional[str] = None,
    max_cols_in_table: int = 200,
) -> str:
    pk_column = pk_column or table_profile.chosen_pk
    md: List[str] = []

    md.append(f"# EDA Notes: {schema_name}.{table_name}")
    md.append("")
    md.append("## Overview")
    md.append(f"- Rows: **{table_profile.n_rows:,}**")
    md.append(f"- Columns: **{table_profile.n_cols:,}**")
    if pk_column:
        md.append(f"- Primary key (chosen / suggested): **{pk_column}**")
    else:
        md.append("- Primary key: **(none)**")
    if table_profile.pk_candidates:
        md.append(f"- Other PK candidates: {', '.join('`'+c+'`' for c in table_profile.pk_candidates[1:10]) or '(none)'}")
    md.append("")

    if table_profile.notes:
        md.append("## Dataset-level notes")
        for n in table_profile.notes:
            md.append(f"- {n}")
        md.append("")

    md.append("## Column summary")
    md.append("")
    md.append("| Column | Pandas dtype | Inferred SQL type | Nullable | Missing % | Distinct | Example values | Notes |")
    md.append("|---|---:|---:|:---:|---:|---:|---|---|")

    profiles = table_profile.column_profiles
    if len(profiles) > max_cols_in_table:
        md.append(f"| *(showing first {max_cols_in_table} of {len(profiles)} columns)* ||||||||")
        profiles = profiles[:max_cols_in_table]

    for p in profiles:
        notes = []
        if p.is_candidate_pk:
            notes.append("PK candidate")
        if p.max_len is not None:
            notes.append(f"max_len={p.max_len}")
        if p.min_value is not None and p.max_value is not None and p.inferred_sql_type.upper().startswith(("INT", "BIGINT", "SMALLINT", "TINYINT", "DECIMAL", "FLOAT", "DATETIME2")):
            notes.append(f"min={p.min_value}")
            notes.append(f"max={p.max_value}")
        notes.extend(p.warnings[:2])

        md.append(
            "| "
            + " | ".join(
                [
                    f"`{_md_escape(p.name)}`",
                    _md_escape(p.pandas_dtype),
                    _md_escape(p.inferred_sql_type),
                    "YES" if p.nullable else "NO",
                    f"{p.missing_pct*100:.2f}%",
                    f"{p.n_unique:,}",
                    _md_escape(", ".join(p.sample_values[:5])),
                    _md_escape("; ".join(notes)) if notes else "",
                ]
            )
            + " |"
        )

    md.append("")
    md.append("## Data quality checks")
    md.append("")

    high_missing = [p for p in table_profile.column_profiles if p.missing_pct >= 0.5]
    if high_missing:
        md.append("### High missingness columns (>= 50%)")
        for p in sorted(high_missing, key=lambda x: x.missing_pct, reverse=True)[:25]:
            md.append(f"- `{p.name}`: {p.missing_pct*100:.1f}% missing")
        md.append("")

    constant_cols = [p for p in table_profile.column_profiles if p.n_unique == 1 and (table_profile.n_rows - p.n_missing) > 0]
    if constant_cols:
        md.append("### Constant columns (only one non-null value)")
        for p in constant_cols[:25]:
            ex = p.sample_values[0] if p.sample_values else ""
            md.append(f"- `{p.name}` (e.g., `{_md_escape(ex)}`)")
        md.append("")

    try:
        dup_rows = int(df.duplicated().sum())
        md.append(f"- Duplicate rows (exact): **{dup_rows:,}**")
    except Exception:
        md.append("- Duplicate rows (exact): *(not computed)*")

    if table_profile.pk_candidates:
        md.append(f"- Candidate key columns: {', '.join('`'+c+'`' for c in table_profile.pk_candidates[:10])}")
    else:
        md.append("- Candidate key columns: *(none found by heuristics)*")

    md.append("")
    md.append("## Suggested follow-ups")
    md.append("")
    md.append("- Validate inferred data types against business meaning (especially for codes/IDs that may need `NVARCHAR`).")
    md.append("- Decide on a *business* primary key vs. a surrogate key (IDENTITY).")
    md.append("- Confirm which columns are used in joins/filters to tune indexes for your workload.")
    md.append("")
    md.append("## Loading notes")
    md.append("")
    md.append("- If you use `BULK INSERT`, ensure the file is accessible *from the SQL Server machine*.")
    md.append("- If your CSV contains commas inside quotes, basic `FIELDTERMINATOR` loading may not be sufficient (depends on SQL Server version).")
    md.append("")

    return "\n".join(md)
