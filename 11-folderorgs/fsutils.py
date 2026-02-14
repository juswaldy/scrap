"""Filesystem helpers.

We centralize file operations so we can:
- implement dry-run uniformly
- implement collision-safe renames
- keep print output consistent
"""

from __future__ import annotations

import errno
import shutil
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Action:
    """A planned filesystem action."""

    kind: str  # "mkdir" | "move"
    src: Path | None
    dst: Path
    note: str = ""


def is_hidden(path: Path) -> bool:
    return any(part.startswith(".") for part in path.parts)


def unique_destination(dst: Path) -> Path:
    """Return a non-colliding destination path.

    If dst exists, append __1, __2, ... before suffix.
    """

    if not dst.exists():
        return dst

    stem = dst.stem
    suffix = dst.suffix
    parent = dst.parent

    i = 1
    while True:
        candidate = parent / f"{stem}__{i}{suffix}"
        if not candidate.exists():
            return candidate
        i += 1


def ensure_dir(path: Path, *, dry_run: bool) -> Action:
    return Action(kind="mkdir", src=None, dst=path)


def move_path(src: Path, dst: Path) -> Action:
    return Action(kind="move", src=src, dst=dst)


def _move_with_retries(src: Path, dst: Path) -> Path:
    """Move src -> dst with retries.

    On Windows/WSL (drvfs), rename/move can transiently fail with PermissionError
    when a file/directory is being scanned or held open by another process.
    """

    # Race-proof: if caller computed dst earlier, it might now exist.
    dst = unique_destination(dst)

    delays_s = (0.0, 0.10, 0.25, 0.50, 1.00)
    last_exc: BaseException | None = None

    for delay in delays_s:
        if delay:
            time.sleep(delay)

        try:
            src.rename(dst)
            return dst
        except FileExistsError:
            dst = unique_destination(dst)
            continue
        except OSError as exc:
            last_exc = exc
            err = getattr(exc, "errno", None)

            # Cross-device move: fall back to shutil.move.
            if err == errno.EXDEV:
                shutil.move(str(src), str(dst))
                return dst

            # Permission/Access denied: retry (often transient on drvfs).
            if isinstance(exc, PermissionError) or err in {errno.EACCES, errno.EPERM}:
                # If destination appeared meanwhile, pick a new name.
                if dst.exists():
                    dst = unique_destination(dst)
                continue

            raise

    # Final fallback: try shutil.move once (may succeed where rename fails).
    try:
        shutil.move(str(src), str(dst))
        return dst
    except Exception as exc:
        raise last_exc or exc


def apply_action(action: Action, *, dry_run: bool) -> None:
    if action.kind == "mkdir":
        if dry_run:
            print(f"[dry-run] mkdir -p {action.dst}")
            return
        action.dst.mkdir(parents=True, exist_ok=True)
        return

    if action.kind == "move":
        assert action.src is not None
        if dry_run:
            print(f"[dry-run] mv {action.src} -> {action.dst}")
            return

        action.dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            _move_with_retries(action.src, action.dst)
        except PermissionError as exc:
            print(f"[warn] permission denied moving {action.src} -> {action.dst}: {exc}")
            return
        return

    raise ValueError(f"Unknown action kind: {action.kind}")
