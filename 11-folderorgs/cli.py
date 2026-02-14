"""CLI entrypoint.

This module parses arguments and executes organization steps.

Design goals:
- Be safe by default (dry-run supported everywhere).
- Be explainable (prints what it will do).
- Be incremental (run step-by-step).
"""

from __future__ import annotations

import argparse
from pathlib import Path

from .organizer import DownloadsOrganizer


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="downloads_organizer",
        description="Organize a Downloads folder using Option A (purpose-first).",
    )

    p.add_argument(
        "--root",
        required=True,
        type=Path,
        help="Root directory to organize (e.g., your Downloads folder).",
    )

    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions but do not move/create anything.",
    )

    p.add_argument(
        "--step",
        default="all",
        choices=["1", "2", "3", "4", "5", "all"],
        help="Which step to run (1-5) or 'all'.",
    )

    p.add_argument(
        "--include-hidden",
        action="store_true",
        help="Also process hidden files/directories (dotfiles).",
    )

    p.add_argument(
        "--verbose",
        action="store_true",
        help="Print additional classification reasoning.",
    )

    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    organizer = DownloadsOrganizer(
        root=args.root,
        dry_run=args.dry_run,
        include_hidden=args.include_hidden,
        verbose=args.verbose,
    )

    if args.step == "all":
        organizer.run_all()
    else:
        step = int(args.step)
        organizer.run_step(step)

    return 0
