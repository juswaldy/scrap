import argparse
from typing import Dict, List, Optional, Callable
from zstd_textfs import compress_folder, decompress_folder

def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Compress and decompress files folders")
    parser.add_argument('--compress', '-c', action='store_true', help="Compress files in the input folder")
    parser.add_argument('--decompress', '-d', action='store_true', help="Decompress files in the input folder")
    parser.add_argument('--input', '-i', required=True, help="Path to input folder")
    parser.add_argument('--output', '-o', required=True, help="Path to output folder")
    args = parser.parse_args(argv)

    if args.compress:
        compress_folder(args.input, args.output)
    elif args.decompress:
        decompress_folder(args.input, args.output)

if __name__ == "__main__":
    main()