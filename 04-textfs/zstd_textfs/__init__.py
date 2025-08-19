"""High-level functions for compressing and decompressing directories using Zstandard.

This package provides a pair of functions, :func:`compress_folder` and
``decompress_folder``, which walk a directory tree, compress the contents of
each file using the Zstandard algorithm and translate the resulting bytes into
a printable form via Base‑64 encoding.  File and directory names are also
compressed and encoded.  The resulting directory structure contains only
alphanumeric characters, hyphens and underscores in its names so it is
readable and navigable on Windows and Linux.  A matching
:func:`decompress_folder` routine reverses the transformation and restores
the original directory tree.

The functions rely on the standard libzstd shared library to perform the
actual compression.  The simple Zstandard API exposes two functions
``ZSTD_compress`` and ``ZSTD_decompress`` for one‑shot compression and
decompression.  As the Zstandard manual explains, ``ZSTD_compress``
compresses an input buffer into a single frame and writes the output into a
provided destination buffer; it returns the compressed size or an error code
if the buffer was too small【524154567153289†L83-L93】.  ``ZSTD_decompress``
reverses the operation, expanding a compressed frame into a destination
buffer of sufficient size【524154567153289†L95-L105】.  To determine the size of the
original data without decompressing, ``ZSTD_getFrameContentSize`` can be
used; it returns either the decompressed size, a special value signalling
"unknown", or an error constant【524154567153289†L107-L118】.  This module wraps
those C functions via :mod:`ctypes` so that Zstandard compression can be
performed without any external Python dependencies.  Should the content size
be unknown, decompression code dynamically increases the output buffer until
the frame can be decoded successfully.

The encoding step uses Base‑64 with the URL‑safe alphabet.  The encoded
names contain only the characters ``A–Z``, ``a–z``, ``0–9``, ``-`` and
``_``.  According to cross‑platform file naming guidelines
alphanumeric characters and the underscore are always safe to use in
filenames; the characters ``\/:*?"<>|`` and the null byte must be avoided
and other punctuation such as spaces and semicolons should be treated with
caution【977998697659771†L387-L414】.  Using the URL‑safe Base‑64 alphabet
ensures that encoded names avoid these problematic characters.

.. warning::

   All files within the input directory will be treated as opaque binary
   streams.  No attempt is made to detect text encodings or skip binary
   formats.  Compressing already‑compressed media (such as images, audio or
   archives) may increase the file size.  Ensure that you have adequate
   backups before running :func:`compress_folder` on important data.

Example
-------

The following example demonstrates how to compress and then decompress a
directory::

    from zstd_textfs import compress_folder, decompress_folder

    # Compress the contents of ``docs`` into ``docs.compressed``
    compress_folder('docs', 'docs.compressed')

    # Later on, restore the original files into ``docs.restored``
    decompress_folder('docs.compressed', 'docs.restored')

Both operations leave the original directory untouched.  The compressed
directory can be navigated in a file manager; each file is encoded as
Base‑64 text and can be viewed in a plain text editor, although its contents
are not human‑readable.
"""

from .core import compress_folder, decompress_folder

__all__ = ["compress_folder", "decompress_folder"]