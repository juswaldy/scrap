"""Core compression and decompression routines for directory trees.

This module contains low‑level functions used by :func:`zstd_textfs.compress_folder`
and :func:`zstd_textfs.decompress_folder`.  The primary responsibilities of
these routines are:

* Loading the system's ``libzstd`` shared library via :mod:`ctypes`.
* Providing Python functions :func:`compress_bytes` and
  :func:`decompress_bytes` that wrap the C API functions
  ``ZSTD_compress`` and ``ZSTD_decompress`` described in the Zstandard
  manual【524154567153289†L83-L105】.  These wrappers handle allocation of
  destination buffers using ``ZSTD_compressBound`` and
  ``ZSTD_getFrameContentSize`` and automatically retry decompression when
  the required output size is unknown【524154567153289†L107-L124】.
* Encoding compressed bytes using Base‑64 with a URL‑safe alphabet and
  removing padding so that encoded file and directory names consist only of
  characters safe on Windows and Linux【977998697659771†L387-L414】.
* Recursively traversing directory trees to map original names to
  compressed names and writing compressed file contents to disk.
* Reversing the operation to restore the original directory structure and
  file contents.

The module exposes two public functions:

``compress_folder(src: str, dst: str, level: int = 3) -> None``
    Compress all files and names in ``src`` into ``dst``.  Existing
    contents in ``dst`` will be overwritten if present.  Directory names
    and file names are encoded using Zstandard followed by Base‑64.  File
    contents are compressed with the same algorithm and encoded as text.

``decompress_folder(src: str, dst: str) -> None``
    Restore a directory previously produced by :func:`compress_folder` back
    to its original form.  Compressed names and contents are decoded and
    decompressed.  The result is written into ``dst``.

These functions do not modify the input directory; instead they create a
new directory tree rooted at the destination.  Directory traversal is
performed using :func:`os.walk`, so symbolic links are followed as
in `os.walk` on your platform.
"""

from __future__ import annotations

import base64
import ctypes
import os
import shutil
from typing import Iterable, Tuple

__all__ = ["compress_folder", "decompress_folder", "compress_bytes", "decompress_bytes"]


# -----------------------------------------------------------------------------
# Compression backends
#
# The core of this module uses Zstandard for compression.  Several possible
# backends are supported so that the library can operate on systems without
# ``libzstd`` installed.  The search order is:
#
# 1. Use the built‑in ``compression.zstd`` module (Python ≥ 3.12) if available.
# 2. Use the third‑party ``zstandard`` package if installed.
# 3. Fallback to calling functions in the system's ``libzstd`` via ``ctypes``.
#
# The fallback backend mirrors the simple C API described in the Zstandard
# manual.  ``ZSTD_compress`` compresses an input buffer into a single frame
#【524154567153289†L83-L93】 and ``ZSTD_decompress`` restores it【524154567153289†L95-L105】.  The helpers
# ``ZSTD_compressBound`` and ``ZSTD_getFrameContentSize`` compute buffer sizes
#【524154567153289†L107-L124】【524154567153289†L168-L174】.

try:
    # Attempt to use the standard library zstd support (Python 3.12+).
    import compression.zstd as _pyzstd  # type: ignore[attr-defined]

    def compress_bytes(data: bytes, level: int = 3) -> bytes:
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("data must be bytes-like")
        return _pyzstd.compress(data, level=level)

    def decompress_bytes(data: bytes) -> bytes:
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError("data must be bytes-like")
        return _pyzstd.decompress(data)

except Exception:
    try:
        # Fallback to third‑party python‑zstandard package if installed.
        import zstandard as _zstd  # type: ignore[import-not-found]

        def compress_bytes(data: bytes, level: int = 3) -> bytes:
            if not isinstance(data, (bytes, bytearray, memoryview)):
                raise TypeError("data must be bytes-like")
            compressor = _zstd.ZstdCompressor(level=level)
            return compressor.compress(bytes(data))

        def decompress_bytes(data: bytes) -> bytes:
            if not isinstance(data, (bytes, bytearray, memoryview)):
                raise TypeError("data must be bytes-like")
            decompressor = _zstd.ZstdDecompressor()
            return decompressor.decompress(bytes(data))

    except Exception:
        # Final fallback: use ctypes to call libzstd directly.
        _libzstd_names = [
            "libzstd.so.1",  # common on Linux
            "libzstd.so",    # fallback
            "zstd.dll",      # Windows
            "libzstd.dylib", # macOS
        ]

        def _load_libzstd() -> ctypes.CDLL:
            for name in _libzstd_names:
                try:
                    return ctypes.CDLL(name)
                except OSError:
                    continue
            raise OSError(
                "Unable to locate libzstd shared library. "
                "Install the Zstandard development package or use Python ≥ 3.12 with compression.zstd."
            )

        _lib = _load_libzstd()

        # Pull in the C API functions and set argument/return types.
        _ZSTD_compress = _lib.ZSTD_compress
        _ZSTD_compress.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int]
        _ZSTD_compress.restype = ctypes.c_size_t

        _ZSTD_decompress = _lib.ZSTD_decompress
        _ZSTD_decompress.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p, ctypes.c_size_t]
        _ZSTD_decompress.restype = ctypes.c_size_t

        _ZSTD_compressBound = _lib.ZSTD_compressBound
        _ZSTD_compressBound.argtypes = [ctypes.c_size_t]
        _ZSTD_compressBound.restype = ctypes.c_size_t

        _ZSTD_isError = _lib.ZSTD_isError
        _ZSTD_isError.argtypes = [ctypes.c_size_t]
        _ZSTD_isError.restype = ctypes.c_uint

        _ZSTD_getErrorName = _lib.ZSTD_getErrorName
        _ZSTD_getErrorName.argtypes = [ctypes.c_size_t]
        _ZSTD_getErrorName.restype = ctypes.c_char_p

        _ZSTD_getFrameContentSize = _lib.ZSTD_getFrameContentSize
        _ZSTD_getFrameContentSize.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        _ZSTD_getFrameContentSize.restype = ctypes.c_ulonglong

        # Constants from the Zstandard manual【524154567153289†L107-L118】.
        ZSTD_CONTENTSIZE_UNKNOWN = (1 << 64) - 1
        ZSTD_CONTENTSIZE_ERROR = (1 << 64) - 2

        def _check_error(code: int) -> None:
            if _ZSTD_isError(code):
                err_ptr = _ZSTD_getErrorName(code)
                err = err_ptr.decode("ascii", "replace") if err_ptr else "unknown error"
                raise RuntimeError(f"Zstandard error: {err}")

        def compress_bytes(data: bytes, level: int = 3) -> bytes:
            if not isinstance(data, (bytes, bytearray, memoryview)):
                raise TypeError("data must be bytes-like")
            src_size = len(data)
            dst_capacity = _ZSTD_compressBound(src_size)
            dst_buffer = ctypes.create_string_buffer(dst_capacity)
            src_buffer = (ctypes.c_char * src_size).from_buffer_copy(data)
            compressed_size = _ZSTD_compress(
                dst_buffer, dst_capacity, src_buffer, src_size, int(level)
            )
            _check_error(compressed_size)
            return dst_buffer.raw[:compressed_size]

        def decompress_bytes(data: bytes) -> bytes:
            if not isinstance(data, (bytes, bytearray, memoryview)):
                raise TypeError("data must be bytes-like")
            src_size = len(data)
            src_buffer = (ctypes.c_char * src_size).from_buffer_copy(data)
            frame_size = _ZSTD_getFrameContentSize(src_buffer, src_size)
            if frame_size == ZSTD_CONTENTSIZE_ERROR:
                raise RuntimeError("Invalid ZSTD frame: cannot determine decompressed size")
            if frame_size != ZSTD_CONTENTSIZE_UNKNOWN:
                dst_buffer = ctypes.create_string_buffer(frame_size)
                decompressed_size = _ZSTD_decompress(
                    dst_buffer, frame_size, src_buffer, src_size
                )
                _check_error(decompressed_size)
                return dst_buffer.raw[:decompressed_size]
            capacity = src_size * 4 or 1
            max_capacity = src_size * 1024 + 1024
            last_error = 0
            while capacity <= max_capacity:
                dst_buffer = ctypes.create_string_buffer(capacity)
                decompressed_size = _ZSTD_decompress(
                    dst_buffer, capacity, src_buffer, src_size
                )
                if not _ZSTD_isError(decompressed_size):
                    return dst_buffer.raw[:decompressed_size]
                last_error = decompressed_size
                capacity *= 2
            # If no capacity worked, raise the last error.
            _check_error(last_error)
            return b""


# -----------------------------------------------------------------------------
# Encoding and decoding helper functions
#
def _b64_encode(data: bytes) -> str:
    """Encode binary data into a URL‑safe Base‑64 string without padding.

    Parameters
    ----------
    data : bytes
        Binary data to encode.

    Returns
    -------
    str
        The Base‑64 encoded string using the URL‑safe alphabet.  Any ``=``
        padding characters are stripped to produce shorter filenames.  When
        decoding, the padding will be restored automatically.
    """
    encoded = base64.urlsafe_b64encode(data).decode("ascii")
    return encoded.rstrip("=")


def _b64_decode(data: str) -> bytes:
    """Decode a URL‑safe Base‑64 string back into binary data.

    The function appends padding characters if necessary so that the
    encoded length is divisible by four, as required by :func:`base64.b64decode`.

    Parameters
    ----------
    data : str
        Base‑64 encoded text without padding.

    Returns
    -------
    bytes
        The decoded binary data.
    """
    # Restore padding for base64 decoding.
    padding = '=' * ((4 - len(data) % 4) % 4)
    return base64.urlsafe_b64decode(data + padding)


def _encode_name(name: str, level: int = 3) -> str:
    """Compress and encode a filesystem name (file or directory).

    The input string is encoded as UTF‑8 bytes, compressed with
    :func:`compress_bytes`, then converted into a URL‑safe Base‑64 string
    without padding.  The resulting string contains only
    alphanumeric characters, hyphens and underscores, which are considered
    safe for cross‑platform filenames【977998697659771†L387-L414】.

    Parameters
    ----------
    name : str
        The original name.
    level : int, optional
        Compression level to pass to :func:`compress_bytes`.

    Returns
    -------
    str
        The compressed and encoded name.
    """
    name_bytes = name.encode("utf-8")
    compressed = compress_bytes(name_bytes, level=level)
    return _b64_encode(compressed)


def _decode_name(name: str) -> str:
    """Decode and decompress a previously encoded filesystem name.

    Parameters
    ----------
    name : str
        The encoded name produced by :func:`_encode_name`.

    Returns
    -------
    str
        The original UTF‑8 name.
    """
    compressed = _b64_decode(name)
    original_bytes = decompress_bytes(compressed)
    return original_bytes.decode("utf-8")


def _compress_file(src_path: str, dst_path: str, level: int = 3) -> None:
    """Read a file, compress its contents and write to ``dst_path``.

    Parameters
    ----------
    src_path : str
        Path to the input file.
    dst_path : str
        Destination path for the compressed and encoded output.
    level : int, optional
        Compression level for :func:`compress_bytes`.
    """
    with open(src_path, 'rb') as f_in:
        data = f_in.read()
    compressed = compress_bytes(data, level=level)
    encoded_text = _b64_encode(compressed)
    # Write encoded text as ASCII.  Use newline for readability.
    with open(dst_path, 'w', encoding='ascii', newline='') as f_out:
        f_out.write(encoded_text)
        print(f"{src_path} --> {dst_path}")


def _decompress_file(src_path: str, dst_path: str) -> None:
    """Read an encoded file, decode and decompress its contents into ``dst_path``.

    Parameters
    ----------
    src_path : str
        Path to the file containing Base‑64 encoded compressed data.
    dst_path : str
        Destination path for the restored binary data.
    """
    with open(src_path, 'r', encoding='ascii') as f_in:
        encoded_text = f_in.read().strip()
    compressed = _b64_decode(encoded_text)
    original = decompress_bytes(compressed)
    # Write bytes back exactly as they were read.
    with open(dst_path, 'wb') as f_out:
        f_out.write(original)
        print(f"{src_path} --> {dst_path}")


def compress_folder(src: str, dst: str, level: int = 3) -> None:
    """Compress an entire directory tree into a new directory.

    This function walks the source directory recursively.  For each directory
    encountered it creates a corresponding directory under ``dst`` whose
    name has been compressed and Base‑64 encoded.  For each file it writes
    a compressed, encoded representation of the file contents using
    :func:`_compress_file`.  Encoded names contain only characters deemed
    safe for cross‑platform filenames【977998697659771†L387-L414】.

    Parameters
    ----------
    src : str
        Path of the directory to compress.
    dst : str
        Path of the directory that will hold the compressed output.  If
        ``dst`` exists it will be removed first.
    level : int, optional
        Compression level for Zstandard.  See :func:`compress_bytes` for
        details.
    """
    # Remove destination if it exists to avoid mixing old and new files.
    if os.path.exists(dst):
        shutil.rmtree(dst)
    os.makedirs(dst, exist_ok=True)
    # Normalize source path.
    src = os.path.abspath(src)
    for root, dirs, files in os.walk(src):
        # Compute relative path from source and encode each part.
        rel_path = os.path.relpath(root, src)
        encoded_parts = [] if rel_path == os.curdir else [
            _encode_name(part, level=level) for part in rel_path.split(os.sep)
        ]
        # Build destination root directory path.
        dst_root = os.path.join(dst, *encoded_parts) if encoded_parts else dst
        os.makedirs(dst_root, exist_ok=True)
        # Process directories.  We don't need to copy contents now; the walk
        # will descend into them automatically.
        for dirname in dirs:
            encoded_dirname = _encode_name(dirname, level=level)
            dir_path = os.path.join(dst_root, encoded_dirname)
            # Ensure directory exists.
            os.makedirs(dir_path, exist_ok=True)
        # Process files.
        for filename in files:
            encoded_filename = _encode_name(filename, level=level)
            src_file_path = os.path.join(root, filename)
            dst_file_path = os.path.join(dst_root, encoded_filename)
            _compress_file(src_file_path, dst_file_path, level=level)


def decompress_folder(src: str, dst: str) -> None:
    """Restore a directory compressed by :func:`compress_folder`.

    The function walks the encoded directory tree and reconstructs the
    original names and file contents.  Encoded names are decoded using
    :func:`_decode_name`, and file contents are decoded and decompressed via
    :func:`_decompress_file`.  The resulting directory structure is written
    to ``dst``.  Existing contents of ``dst`` will be removed before
    decompression begins.

    Parameters
    ----------
    src : str
        Path of the directory produced by :func:`compress_folder`.
    dst : str
        Destination path where the original directory should be restored.
    """
    if os.path.exists(dst):
        shutil.rmtree(dst)
    os.makedirs(dst, exist_ok=True)
    src = os.path.abspath(src)
    for root, dirs, files in os.walk(src):
        rel_path = os.path.relpath(root, src)
        decoded_parts = [] if rel_path == os.curdir else [
            _decode_name(part) for part in rel_path.split(os.sep)
        ]
        dst_root = os.path.join(dst, *decoded_parts) if decoded_parts else dst
        os.makedirs(dst_root, exist_ok=True)
        # Create decoded directories.
        for dirname in dirs:
            decoded_dirname = _decode_name(dirname)
            dir_path = os.path.join(dst_root, decoded_dirname)
            os.makedirs(dir_path, exist_ok=True)
        # Decode files.
        for filename in files:
            decoded_filename = _decode_name(filename)
            src_file_path = os.path.join(root, filename)
            dst_file_path = os.path.join(dst_root, decoded_filename)
            _decompress_file(src_file_path, dst_file_path)