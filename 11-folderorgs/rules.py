"""Classification rules.

These heuristics are intentionally simple and explainable; you can extend them.

The organizer uses multiple passes:
- Pass 1: obvious by extension/category (media/software/archives/backups/logs)
- Pass 2: work vs reading (keywords + some extensions)
- Pass 3: domain routing (finance/student/identity/integrations/etc.)

Note: this is not an ML classifier; it's deterministic rules.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


MEDIA_IMAGE = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".svg", ".webp", ".tiff"}
MEDIA_VIDEO = {".mp4", ".mov", ".m4v", ".mkv", ".avi", ".webm"}
MEDIA_AUDIO = {".mp3", ".wav", ".flac", ".m4a", ".aac", ".ogg"}

ARCHIVES = {".zip", ".7z", ".tar", ".gz", ".tgz", ".rar"}

INSTALLERS = {".msi", ".exe", ".vsix", ".appinstaller"}
ISOS = {".iso"}

BACKUPS = {".bak", ".dacpac", ".ispac"}
LOGS = {".log", ".oxps"}

NOTEBOOKS = {".ipynb"}
CODE = {".py", ".rb", ".js", ".ts", ".go", ".java", ".cs", ".ps1", ".sh"}
DATA = {".csv", ".tsv", ".xlsx", ".xls", ".xlsm", ".xlsb", ".json", ".xml", ".yaml", ".yml"}
SQL = {".sql"}
DOCS = {".pdf", ".docx", ".pptx", ".md", ".txt", ".html", ".htm"}


WORK_KEYWORDS = {
    "twu",
    "jenzabar",
    "clover",
    "salesforce",
    "integration",
    "student",
    "housing",
    "enrollment",
    "finance",
    "trial",
    "tuition",
    "vena",
    "payee",
    "reconcile",
    "ledger",
    "gl",
    "ar",
    "ap",
    "ssrs",
    "db",
    "backup",
    "prod",
    "test",
    "staging",
    "aq",
    "j1",
    "jics",
    "entra",
    "active directory",
    "email",
}

READING_KEYWORDS = {
    "paper",
    "article",
    "report",
    "guide",
    "handbook",
    "book",
    "thesis",
    "proceedings",
    "cvpr",
    "arxiv",
    "pnas",
    "nature",
    "springer",
    "oreilly",
}


DOMAIN_KEYWORDS = {
    "Finance": {
        "finance",
        "gl",
        "ledger",
        "trial",
        "balance",
        "tuition",
        "revenue",
        "ar ",
        " ap",
        "payee",
        "positivepay",
        "t2202",
        "t4a",
        "vena",
    },
    "Student": {"student", "housing", "residence", "enrol", "enroll", "candidacy", "course"},
    "Identity": {"entra", "adp", "active directory", "email", "sso", "ldap", "oauth"},
    "Integrations": {"clover", "jenzabar", "salesforce", "wsdl", "sftp", "ssis", "api"},
    "AI-ML": {"ai", "ml", "llm", "neural", "graph", "rag", "nlp", "vision", "transformer"},
    "Security": {"security", "log4shell", "cert", "rsa", "key", "jks"},
}


@dataclass(frozen=True)
class Pass1Result:
    bucket: str | None  # "Images" | "Video" | "Audio" | "Software" | "Archives" | "Backups" | "Logs" | None


def pass1_bucket(path: Path) -> Pass1Result:
    s = path.suffix.lower()

    if s in MEDIA_IMAGE:
        return Pass1Result(bucket="Images")
    if s in MEDIA_VIDEO:
        return Pass1Result(bucket="Video")
    if s in MEDIA_AUDIO:
        return Pass1Result(bucket="Audio")

    if s in INSTALLERS or s in ISOS:
        return Pass1Result(bucket="Software")

    if s in ARCHIVES:
        return Pass1Result(bucket="Archives")

    if s in BACKUPS:
        return Pass1Result(bucket="Backups")

    if s in LOGS:
        return Pass1Result(bucket="Logs")

    return Pass1Result(bucket=None)


def looks_like_reading(path: Path) -> bool:
    name = path.name.lower()
    if path.suffix.lower() in {".pdf", ".epub"}:
        # Many PDFs in Downloads are reading; keywords help disambiguate.
        if any(k in name for k in WORK_KEYWORDS):
            return False
        if any(k in name for k in READING_KEYWORDS):
            return True
        # Default PDFs to Reading unless work keywords present.
        return True

    # Non-PDF reading-like docs
    if path.suffix.lower() in {".md", ".txt"} and any(k in name for k in READING_KEYWORDS):
        return True

    return False


def looks_like_work(path: Path) -> bool:
    name = path.name.lower()
    if any(k in name for k in WORK_KEYWORDS):
        return True

    # SQL + notebooks are usually work in this folder.
    if path.suffix.lower() in SQL or path.suffix.lower() in NOTEBOOKS:
        return True

    return False


def domain_for(path: Path) -> str | None:
    name = path.name.lower()
    for domain, keys in DOMAIN_KEYWORDS.items():
        if any(k in name for k in keys):
            return domain
    return None


def is_project_like_dir(path: Path) -> bool:
    """Heuristic for folders that should move intact under 02-Projects.

    Typical signals:
    - contains many code files
    - has common project markers
    - directory name suggests a project/tool
    """

    if not path.is_dir():
        return False

    lname = path.name.lower()
    if any(x in lname for x in ["adapter", "server", "webapi", "app", "clover", "project", "sandbox", "dx"]):
        return True

    markers = {"pyproject.toml", "requirements.txt", "package.json", ".git", "go.mod", "pom.xml", "*.sln"}
    for m in markers:
        if any(path.glob(m)):
            return True

    # Count a few code-like files (bounded work)
    code_count = 0
    for p in path.rglob("*"):
        if p.is_file() and p.suffix.lower() in (CODE | SQL | NOTEBOOKS):
            code_count += 1
            if code_count >= 10:
                return True

    return False
