"""Core organization logic.

Implements the plan's 5-step approach for Option A (purpose-first).

Steps:
1) Create the folder structure.
2) Sweep root into 00-Inbox (optional but recommended).
3) Pass 1: Obvious buckets (Media/Software/Archives/Ops).
4) Pass 2: Work vs Reading.
5) Pass 3: Domain routing + projects.

The organizer is conservative:
- It won't touch itself (downloads_organizer/) or 00-plan.md.
- It will not follow symlinks.
- It renames on collisions.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from .fsutils import Action, apply_action, ensure_dir, is_hidden, move_path, unique_destination
from .paths import PurposePaths, required_directories
from . import rules


@dataclass
class DownloadsOrganizer:
    root: Path
    dry_run: bool = False
    include_hidden: bool = False
    verbose: bool = False

    def __post_init__(self) -> None:
        self.root = self.root.expanduser().resolve()
        if not self.root.exists() or not self.root.is_dir():
            raise ValueError(f"--root must be an existing directory: {self.root}")

        self.p = PurposePaths(self.root)
        self._protected_names = {
            "00-plan.md",
            "downloads_organizer",
        }

    def run_all(self) -> None:
        for step in range(1, 6):
            self.run_step(step)

    def run_step(self, step: int) -> None:
        self._print_step_header(step)
        if step == 1:
            self.step1_create_structure()
        elif step == 2:
            self.step2_sweep_into_inbox()
        elif step == 3:
            self.step3_pass1_obvious_buckets()
        elif step == 4:
            self.step4_pass2_work_vs_reading()
        elif step == 5:
            self.step5_pass3_domains_and_projects()
        else:
            raise ValueError("step must be in 1..5")

        print(f"[done] step {step}")

    def _print_step_header(self, step: int) -> None:
        titles = {
            1: "Create folder structure",
            2: "Sweep root into 00-Inbox",
            3: "Pass 1: Obvious buckets",
            4: "Pass 2: Work vs Reading",
            5: "Pass 3: Domains and Projects",
        }
        title = titles.get(step, "")
        if title:
            print(f"\n== Step {step}/5: {title} ==")
        else:
            print(f"\n== Step {step}/5 ==")

    def _progress_interval(self, total: int) -> int:
        # Aim for ~20 updates max, but show every item for small totals.
        if total <= 20:
            return 1
        return max(1, total // 20)

    def _iter_with_progress(self, items: list[Path], label: str) -> Iterator[tuple[int, int, Path]]:
        total = len(items)
        interval = self._progress_interval(total)
        for idx, item in enumerate(items, start=1):
            if idx == 1 or idx == total or (idx % interval == 0):
                pct = int((idx / total) * 100) if total else 100
                print(f"[progress] {label}: {idx}/{total} ({pct}%)")
            yield idx, total, item

    # --------------------
    # Step 1
    # --------------------
    def step1_create_structure(self) -> None:
        dirs = list(required_directories(self.p))
        print(f"[info] ensuring {len(dirs)} directories")
        for _, _, d in self._iter_with_progress(dirs, "step1"):
            apply_action(ensure_dir(d, dry_run=self.dry_run), dry_run=self.dry_run)

    # --------------------
    # Step 2
    # --------------------
    def step2_sweep_into_inbox(self) -> None:
        """Move most root-level items into 00-Inbox.

        This is the recommended first action after creating folders. It reduces
        clutter and makes subsequent passes operate on a single staging area.
        """

        apply_action(ensure_dir(self.p.inbox, dry_run=self.dry_run), dry_run=self.dry_run)

        items = [item for item in self.root.iterdir() if not self._should_skip_root_item(item)]
        print(f"[info] sweeping {len(items)} items into {self.p.inbox.name}")

        moved = 0
        for _, _, item in self._iter_with_progress(items, "step2"):
            dst = unique_destination(self.p.inbox / item.name)
            apply_action(move_path(item, dst), dry_run=self.dry_run)
            moved += 1
        print(f"[info] moved {moved}/{len(items)} items")

    def _should_skip_root_item(self, item: Path) -> bool:
        if item.name in self._protected_names:
            return True
        if item.name in {
            self.p.inbox.name,
            self.p.work.name,
            self.p.projects.name,
            self.p.data.name,
            self.p.reading.name,
            self.p.media.name,
            self.p.software.name,
            self.p.ops.name,
            self.p.archive.name,
        }:
            return True
        if (not self.include_hidden) and is_hidden(item):
            return True
        # Don't move Windows metadata file
        if item.name.lower() in {"desktop.ini"}:
            return True
        # Avoid crossing filesystems or following links
        if item.is_symlink():
            return True
        return False

    # --------------------
    # Step 3
    # --------------------
    def step3_pass1_obvious_buckets(self) -> None:
        """Move obvious file types out of Inbox.

        Buckets:
        - Media: Images/Video/Audio
        - Software: installers/isos
        - Archives: zip/tar/etc → 99-Archive/Archives
        - Ops: Backups/Logs
        """

        inbox = self.p.inbox
        if not inbox.exists():
            print("00-Inbox does not exist; run step 2 first (or create it).")
            return

        archives_dir = self.p.archive / "Archives"
        apply_action(ensure_dir(archives_dir, dry_run=self.dry_run), dry_run=self.dry_run)

        items = [item for item in list(inbox.iterdir()) if not self._should_skip_inbox_item(item)]
        print(f"[info] scanning {len(items)} items in {inbox.name}")

        moved = 0
        for _, _, item in self._iter_with_progress(items, "step3"):

            res = rules.pass1_bucket(item)
            if res.bucket is None:
                continue

            dst = None
            if res.bucket in {"Images", "Video", "Audio"}:
                dst = unique_destination((self.p.media / res.bucket) / item.name)
            elif res.bucket == "Software":
                if item.suffix.lower() in rules.ISOS:
                    dst = unique_destination((self.p.software / "ISOs") / item.name)
                elif item.suffix.lower() in rules.INSTALLERS:
                    dst = unique_destination((self.p.software / "Installers") / item.name)
                else:
                    dst = unique_destination((self.p.software / "Installers") / item.name)
            elif res.bucket == "Archives":
                dst = unique_destination(archives_dir / item.name)
            elif res.bucket == "Backups":
                dst = unique_destination((self.p.ops / "DB-Backups") / item.name)
            elif res.bucket == "Logs":
                dst = unique_destination((self.p.ops / "Logs") / item.name)

            if dst is not None:
                apply_action(move_path(item, dst), dry_run=self.dry_run)
                moved += 1

        print(f"[info] moved {moved}/{len(items)} items")

    # --------------------
    # Step 4
    # --------------------
    def step4_pass2_work_vs_reading(self) -> None:
        """Separate remaining Inbox items into Work vs Reading.

        Conservative rules:
        - Most PDFs default to Reading unless work keywords present.
        - SQL and notebooks default to Work.
        """

        inbox = self.p.inbox
        if not inbox.exists():
            print("00-Inbox does not exist; run step 2 first.")
            return

        items = [item for item in list(inbox.iterdir()) if not self._should_skip_inbox_item(item)]
        print(f"[info] scanning {len(items)} items in {inbox.name}")

        moved = 0
        for _, _, item in self._iter_with_progress(items, "step4"):

            # Keep directories for step 5 (projects/domains)
            if item.is_dir():
                continue

            if rules.looks_like_reading(item):
                dst = unique_destination(self.p.reading / item.name)
                self._log_reason(item, f"Pass2 -> Reading")
                apply_action(move_path(item, dst), dry_run=self.dry_run)
                moved += 1
                continue

            if rules.looks_like_work(item):
                dst = unique_destination(self.p.work / item.name)
                self._log_reason(item, f"Pass2 -> Work")
                apply_action(move_path(item, dst), dry_run=self.dry_run)
                moved += 1
                continue

        print(f"[info] moved {moved}/{len(items)} items")

    # --------------------
    # Step 5
    # --------------------
    def step5_pass3_domains_and_projects(self) -> None:
        """Route project-like directories and then domain-route remaining items.

        This step is intentionally simple:
        - Any folder that looks like a project gets moved intact to 02-Projects.
        - Remaining inbox items get domain keyword routing into Work or Data.
        """

        inbox = self.p.inbox
        if not inbox.exists():
            print("00-Inbox does not exist; run step 2 first.")
            return

        items = [item for item in list(inbox.iterdir()) if not self._should_skip_inbox_item(item)]
        print(f"[info] scanning {len(items)} items in {inbox.name}")

        # 5a) Move project-like directories intact
        moved_projects = 0
        for _, _, item in self._iter_with_progress(items, "step5a"):
            if item.is_dir() and rules.is_project_like_dir(item):
                dst = unique_destination(self.p.projects / item.name)
                self._log_reason(item, "Pass3 -> Projects (project-like dir)")
                apply_action(move_path(item, dst), dry_run=self.dry_run)
                moved_projects += 1

        print(f"[info] moved {moved_projects} project folders")

        # 5b) Domain-route remaining items
        items2 = [item for item in list(inbox.iterdir()) if not self._should_skip_inbox_item(item)]
        moved_domains = 0
        for _, _, item in self._iter_with_progress(items2, "step5b"):

            dom = rules.domain_for(item)
            if dom is None:
                continue

            # Decide Work vs Data based on extension
            ext = item.suffix.lower()
            if ext in rules.DATA:
                dst = unique_destination((self.p.data / dom) / item.name)
                self._log_reason(item, f"Pass3 -> Data/{dom}")
            else:
                dst = unique_destination((self.p.work / dom) / item.name)
                self._log_reason(item, f"Pass3 -> Work/{dom}")

            apply_action(move_path(item, dst), dry_run=self.dry_run)
            moved_domains += 1

        print(f"[info] moved {moved_domains}/{len(items2)} items")

    def _should_skip_inbox_item(self, item: Path) -> bool:
        if (not self.include_hidden) and is_hidden(item):
            return True
        if item.is_symlink():
            return True
        return False

    def _log_reason(self, item: Path, msg: str) -> None:
        if self.verbose:
            print(f"[classify] {item.name}: {msg}")
