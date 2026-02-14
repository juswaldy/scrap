"""Folder layout for Option A (purpose-first)."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PurposePaths:
    root: Path

    @property
    def inbox(self) -> Path:
        return self.root / "00-Inbox"

    @property
    def work(self) -> Path:
        return self.root / "01-Work"

    @property
    def projects(self) -> Path:
        return self.root / "02-Projects"

    @property
    def data(self) -> Path:
        return self.root / "03-Data"

    @property
    def reading(self) -> Path:
        return self.root / "04-Reading"

    @property
    def media(self) -> Path:
        return self.root / "05-Media"

    @property
    def software(self) -> Path:
        return self.root / "06-Software"

    @property
    def ops(self) -> Path:
        return self.root / "07-Ops-Backups"

    @property
    def archive(self) -> Path:
        return self.root / "99-Archive"


def required_directories(p: PurposePaths) -> list[Path]:
    """Minimal directory set (top-level + a few common subfolders)."""

    return [
        p.inbox,
        p.work,
        p.projects,
        p.data,
        p.reading,
        p.media / "Images",
        p.media / "Video",
        p.media / "Audio",
        p.software / "Installers",
        p.software / "ISOs",
        p.software / "Drivers-SDKs",
        p.ops / "DB-Backups",
        p.ops / "Logs",
        p.ops / "Support-Bundles",
        p.ops / "Certificates-Keys",
        p.archive,
    ]
