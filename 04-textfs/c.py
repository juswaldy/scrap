import argparse
from typing import Dict, List, Optional, Callable
from zstd_textfs import compress_folder, decompress_folder
import os
import filecmp

def folders_are_identical(folder1: str, folder2: str) -> bool:
    """
    Check if two folders and their contents are identical.
    
    Args:
        folder1 (str): Path to the first folder.
        folder2 (str): Path to the second folder.
    
    Returns:
        bool: True if folders are identical, False otherwise.
    """
    # Compare the directories structure and file names recursively
    comparison = filecmp.dircmp(folder1, folder2)
    
    if comparison.left_only or comparison.right_only or comparison.funny_files:
        # Files or folders that exist only in one folder or problematic files found
        return False

    # Recursively check for common subfolders
    for subdir in comparison.common_dirs:
        subfolder1 = os.path.join(folder1, subdir)
        subfolder2 = os.path.join(folder2, subdir)
        if not folders_are_identical(subfolder1, subfolder2):
            return False
    
    # Check common files for content differences
    (match, mismatch, errors) = filecmp.cmpfiles(folder1, folder2, comparison.common_files, shallow=False)
    
    if mismatch or errors:
        return False
    
    return True


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Compress and decompress files folders")
    parser.add_argument('--compress', '-c', action='store_true', help="Compress files in the input folder")
    parser.add_argument('--decompress', '-d', action='store_true', help="Decompress files in the input folder")
    parser.add_argument('--input', '-i', required=True, help="Path to input folder")
    parser.add_argument('--output', '-o', required=True, help="Path to output folder")
    parser.add_argument('--level', '-l', type=int, default=3, help="Compression level -7 to 22 (default: 3)")
    parser.add_argument('--verify', '-v', action='store_true', help="Verify decompressed output against original files")
    args = parser.parse_args(argv)

    if args.compress:
        compress_folder(args.input, args.output, args.level)
    elif args.decompress:
        decompress_folder(args.input, args.output)
    elif args.verify:
        print(folders_are_identical(args.input, args.output))

if __name__ == "__main__":
    main()
