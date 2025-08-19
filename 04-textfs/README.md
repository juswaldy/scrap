Here is a short and sweet README for your project with usage examples:

***

# ZstdTextFS

A lightweight Python package for **compressing and decompressing directories** using the Zstandard algorithm with Base64-encoded filenames and contents. It preserves directory structure and produces filenames safe for Windows and Linux.

## Features

- Compress entire directory trees recursively
- Compress both filenames and file contents
- Output directory with Base64 URL-safe encoded names
- Decompress back to original directory structure
- Uses system `libzstd` without external Python dependencies
- Cross-platform safe filenames

## Installation

Requires `libzstd` installed on your system.

```bash
# On Debian/Ubuntu
sudo apt-get install libzstd-dev

# On macOS with Homebrew
brew install zstd
```

No additional Python dependencies necessary.

## Usage

```python
from zstdtextfs import compressfolder, decompressfolder

# Compress a directory
compressfolder('path/to/original_folder', 'path/to/compressed_folder')

# Decompress the directory
decompressfolder('path/to/compressed_folder', 'path/to/restored_folder')
```

Example:

```python
compressfolder('docs', 'docs.compressed')
decompressfolder('docs.compressed', 'docs.restored')
```

Both operations **leave the original directory untouched**. The compressed directory can be browsed normally, with files encoded as Base64 text (not human-readable).

## Command Line Interface

```bash
python c.py --compress -i input_folder -o output_folder
python c.py --decompress -i input_folder -o output_folder
python c.py --verify -i original_folder -o decompressed_folder
```

- `--compress` or `-c`: Compress input folder
- `--decompress` or `-d`: Decompress input folder
- `-i`, `--input`: Specify input folder path
- `-o`, `--output`: Specify output folder path
- `-l`, `--level`: Compression level (default: 3)
- `-v`, `--verify`: Check if decompressed folder matches original

## Notes

- All files are treated as opaque binary streams; no text encoding detection.
- Compressing already compressed files (like images, archives) may increase size.
- Ensure you have backups before compressing important data.

