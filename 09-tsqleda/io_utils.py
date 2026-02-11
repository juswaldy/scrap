"""
io_utils.py
Shared file-reading helpers for CSV/XLSX with encoding fallbacks.

Goal: avoid common Windows CSV encoding issues (cp1252/latin1) while still preferring UTF-8.
"""
from __future__ import annotations

import io
import pathlib
from typing import Any, Dict, Optional, Tuple, Union

import pandas as pd


def _dedup(seq):
    seen = set()
    out = []
    for x in seq:
        if x is None:
            continue
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def read_csv_best_effort(
    source: Union[str, bytes, io.BytesIO],
    *,
    encoding: Optional[str] = None,
    sep: Optional[str] = None,
    dtype_backend: Optional[str] = "numpy_nullable",
    **kwargs,
) -> Tuple[pd.DataFrame, str]:
    """
    Read a CSV using several encodings until one works.

    Returns (df, used_encoding).

    Parameters
    ----------
    source: path, bytes, or BytesIO
    encoding: if provided, tried first
    sep: delimiter (default lets pandas infer, but we usually pass ',')
    dtype_backend: try pandas nullable backend when supported
    """
    # pandas accepts file paths or file-like objects. Normalize to buffer when bytes given.
    buf = source
    if isinstance(source, (bytes, bytearray)):
        buf = io.BytesIO(source)

    encodings_to_try = _dedup([
        encoding,
        "utf-8",
        "utf-8-sig",
        "cp1252",
        "latin1",
    ])

    # Common kwargs
    read_kwargs: Dict[str, Any] = {}
    read_kwargs.update(kwargs)
    if sep is not None:
        read_kwargs["sep"] = sep

    last_err = None
    for enc in encodings_to_try:
        try:
            if dtype_backend:
                try:
                    df = pd.read_csv(buf, encoding=enc, dtype_backend=dtype_backend, **read_kwargs)
                except TypeError:
                    # pandas < 2.0 doesn't support dtype_backend
                    df = pd.read_csv(buf, encoding=enc, **read_kwargs)
            else:
                df = pd.read_csv(buf, encoding=enc, **read_kwargs)
            return df, enc
        except UnicodeDecodeError as e:
            last_err = e
            # Reset buffer position if needed
            try:
                buf.seek(0)
            except Exception:
                pass
            continue

    # Final fallback: decode with replacement and parse from string buffer
    # This avoids hard crashes, but you should validate the content.
    try:
        if isinstance(source, str):
            data = pathlib.Path(source).read_bytes()
        elif isinstance(source, io.BytesIO):
            source.seek(0)
            data = source.read()
        else:
            data = bytes(source)
    except Exception:
        raise last_err or UnicodeDecodeError("utf-8", b"", 0, 1, "Failed to decode CSV")

    text = data.decode(encodings_to_try[-1], errors="replace")
    df = pd.read_csv(io.StringIO(text), sep=sep or ",", **read_kwargs)
    return df, encodings_to_try[-1]


def read_excel_best_effort(
    source: Union[str, bytes, io.BytesIO],
    *,
    sheet_name: Optional[Union[str, int]] = 0,
    **kwargs,
) -> pd.DataFrame:
    """
    Read XLSX/XLS.
    """
    if isinstance(source, (bytes, bytearray)):
        source = io.BytesIO(source)
    return pd.read_excel(source, sheet_name=sheet_name, **kwargs)


def detect_row_terminator_from_bytes(data: bytes, sample_bytes: int = 1_000_000) -> str:
    """
    Return a SQL Server ROWTERMINATOR value (hex form) based on the data.
    - If CRLF appears, use 0x0d0a
    - Else, use 0x0a
    """
    chunk = data[:sample_bytes]
    if b"\r\n" in chunk:
        return "0x0d0a"
    return "0x0a"


def detect_row_terminator_from_path(path: str, sample_bytes: int = 1_000_000) -> str:
    p = pathlib.Path(path)
    try:
        data = p.read_bytes()[:sample_bytes]
    except Exception:
        # safe default
        return "0x0d0a"
    return detect_row_terminator_from_bytes(data, sample_bytes=sample_bytes)
