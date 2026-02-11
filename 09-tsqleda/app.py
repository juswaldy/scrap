"""
app.py (Streamlit UI)
Upload CSV/XLSX and generate:
- CREATE TABLE
- CREATE INDEX suggestions
- BULK INSERT script (CSV-oriented; for XLSX it gives a template)
- EDA markdown

Run:
  pip install -r requirements.txt
  streamlit run app.py
"""
from __future__ import annotations

import io
import pathlib

import pandas as pd
import streamlit as st

from generator import (
    profile_dataframe,
    generate_create_table_sql,
    generate_create_indexes_sql,
    generate_bulk_insert_sql,
    generate_eda_markdown,
)
from io_utils import read_csv_best_effort, read_excel_best_effort, detect_row_terminator_from_bytes


def read_uploaded(uploaded_file, sheet_name=None, *, encoding=None, sep=","):
    name = uploaded_file.name.lower()
    data = uploaded_file.getvalue()
    if name.endswith(".csv") or name.endswith(".txt"):
        df, used_enc = read_csv_best_effort(data, encoding=encoding, sep=sep)
        return df, used_enc, data
    if name.endswith(".xlsx") or name.endswith(".xls"):
        df = read_excel_best_effort(data, sheet_name=sheet_name or 0)
        return df, None, data
    raise ValueError("Unsupported file type. Upload a CSV or XLSX.")


def main():
    st.set_page_config(page_title="CSV/XLSX → T-SQL + EDA", layout="wide")
    st.title("CSV/XLSX → T-SQL CREATE TABLE/INDEX + BULK INSERT + EDA Markdown")

    uploaded = st.file_uploader("Upload a CSV or XLSX", type=["csv", "txt", "xlsx", "xls"])
    if not uploaded:
        st.info("Upload a file to begin.")
        return

    sheet = None
    if uploaded.name.lower().endswith((".xlsx", ".xls")):
        xls = pd.ExcelFile(io.BytesIO(uploaded.getvalue()))
        sheet = st.selectbox("Sheet", xls.sheet_names, index=0)

    with st.sidebar:
        st.header("Output settings")
        default_table = pathlib.Path(uploaded.name).stem
        schema = st.text_input("Schema", value="dbo")
        table = st.text_input("Table name", value=default_table)
        max_scan = st.number_input("Max values scanned per column", min_value=10_000, max_value=2_000_000, value=200_000, step=10_000)

        st.subheader("CSV settings")
        sep = st.text_input("Delimiter (sep)", value=",")
        encoding = st.text_input("Encoding (blank = auto fallbacks)", value="")
        has_header = st.checkbox("CSV has header row", value=True)

        st.subheader("BULK INSERT settings")
        bulk_path = st.text_input("Path visible to SQL Server", value=r"<PATH_VISIBLE_TO_SQL_SERVER>")
        bulk_codepage = st.text_input("CODEPAGE (optional)", value="")  # e.g. 65001, 1252

    try:
        df, used_enc, raw_bytes = read_uploaded(uploaded, sheet_name=sheet, encoding=(encoding or None), sep=sep)
    except Exception as e:
        st.error(f"Failed to read file: {e}")
        return

    st.subheader("Preview")
    st.dataframe(df.head(50), use_container_width=True)

    tp = profile_dataframe(df, max_scan_per_column=int(max_scan))

    st.subheader("Inferred schema & profiling")
    prof_rows = []
    for p in tp.column_profiles:
        prof_rows.append({
            "Column": p.name,
            "Pandas dtype": p.pandas_dtype,
            "Inferred SQL type": p.inferred_sql_type,
            "Nullable": p.nullable,
            "Missing %": round(p.missing_pct * 100, 2),
            "Distinct": p.n_unique,
            "Unique?": p.is_unique,
            "PK candidate": p.is_candidate_pk,
            "Examples": ", ".join(p.sample_values[:5]),
            "Warnings": "; ".join(p.warnings[:2]),
        })
    st.dataframe(pd.DataFrame(prof_rows), use_container_width=True, height=420)

    st.subheader("Primary key choice")
    pk_options = ["(none)"] + [p.name for p in tp.column_profiles]
    suggested = tp.chosen_pk if tp.chosen_pk else "(none)"
    default_idx = pk_options.index(suggested) if suggested in pk_options else 0
    pk_choice = st.selectbox("Primary key column", pk_options, index=default_idx)
    add_surrogate = st.checkbox("Add surrogate BIGINT IDENTITY primary key", value=(pk_choice == "(none)"))

    effective_pk = None if pk_choice == "(none)" else pk_choice
    if add_surrogate:
        effective_pk = f"{table}_sk"

    create_table_sql = generate_create_table_sql(
        tp,
        schema_name=schema,
        table_name=table,
        pk_column=None if pk_choice == "(none)" else pk_choice,
        add_surrogate_pk=add_surrogate,
    )
    create_indexes_sql = generate_create_indexes_sql(
        tp,
        schema_name=schema,
        table_name=table,
        pk_column=effective_pk,
    )
    eda_md = generate_eda_markdown(
        df,
        tp,
        schema_name=schema,
        table_name=table,
        pk_column=effective_pk,
    )

    # Bulk insert script
    # If the input is CSV, we can infer newline style; for XLSX we still emit a template.
    name = uploaded.name.lower()
    if name.endswith((".csv", ".txt")):
        rowterm = detect_row_terminator_from_bytes(raw_bytes)
        firstrow = 2 if has_header else 1
        codepage = int(bulk_codepage) if bulk_codepage.strip().isdigit() else None
        bulk_sql = generate_bulk_insert_sql(
            schema,
            table,
            bulk_path,
            first_row=firstrow,
            field_terminator=sep,
            row_terminator=rowterm,
            codepage=codepage,
        )
    else:
        bulk_sql = (
            "-- This is a template. BULK INSERT cannot read XLSX directly.\n"
            "-- Export your Excel sheet to CSV, copy it to the SQL Server machine or a UNC share,\n"
            "-- then update the FROM path below.\n\n"
            + generate_bulk_insert_sql(
                schema,
                table,
                bulk_path,
                first_row=2,
                field_terminator=sep,
                row_terminator="0x0d0a",
                codepage=int(bulk_codepage) if bulk_codepage.strip().isdigit() else None,
            )
        )

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("CREATE TABLE (T-SQL)")
        st.code(create_table_sql, language="sql")
        st.download_button(
            "Download create_table.sql",
            data=create_table_sql.encode("utf-8"),
            file_name=f"{table}.create_table.sql",
            mime="text/plain",
        )

        st.subheader("CREATE INDEX (T-SQL)")
        st.code(create_indexes_sql, language="sql")
        st.download_button(
            "Download create_indexes.sql",
            data=create_indexes_sql.encode("utf-8"),
            file_name=f"{table}.create_indexes.sql",
            mime="text/plain",
        )

    with col2:
        st.subheader("BULK INSERT (T-SQL)")
        st.code(bulk_sql, language="sql")
        st.download_button(
            "Download bulk_insert.sql",
            data=bulk_sql.encode("utf-8"),
            file_name=f"{table}.bulk_insert.sql",
            mime="text/plain",
        )

        st.subheader("EDA notes (Markdown)")
        st.code(eda_md, language="markdown")
        st.download_button(
            "Download eda.md",
            data=eda_md.encode("utf-8"),
            file_name=f"{table}.eda.md",
            mime="text/plain",
        )

    if used_enc:
        st.caption(f"CSV decoded using encoding: **{used_enc}**")


if __name__ == "__main__":
    main()
