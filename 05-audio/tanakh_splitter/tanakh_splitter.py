#!/usr/bin/env python3
"""tanakh_splitter.py

Split a Tanakh (Hebrew Bible) chapter MP3 into per-verse MP3 files.

Core features
- Reads the book+chapter from the input filename (e.g. exo-12.mp3 -> Exodus 12)
- Downloads Hebrew verse text for that chapter (default: Sefaria Texts API)
- Detects silence boundaries in the audio using FFmpeg's `silencedetect`
- Chooses exactly (verse_count - 1) boundaries and splits the audio
- Writes verse MP3s like: exo-12-01.mp3 ... exo-12-51.mp3
- Writes a manifest JSON with verse timings + Hebrew text

Minimum parameters
  1) path to chapter mp3 (named book-chapter.mp3)
  2) output folder

Example
  python tanakh_splitter.py ./exo-12.mp3 ./out

Requirements
- Python 3.9+
- FFmpeg + ffprobe available on PATH
- Internet access on first run (for Hebrew text fetch) OR a populated cache

Notes
- This script is designed for chapter recordings with an audible pause between verses.
- Silence detection parameters vary by recording. The script auto-tries multiple
  thresholds, but you can override with --noise-db and --detect-d.

Text source
- By default we use the (deprecated but still widely supported) Sefaria Texts v1 endpoint:
    https://www.sefaria.org/api/texts/{tref}?context=0&commentary=0&pad=0
  (see Sefaria API docs for parameters).

"""

from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


# -----------------------------
# Book mapping
# -----------------------------
# The MP3 filename is expected to start with a short code (like exo, gen, 1sa, etc.).
# Map those codes to Sefaria book titles.
#
# You can add aliases freely.
BOOK_CODE_TO_TITLE: Dict[str, str] = {
    # Torah
    "gen": "Genesis",
    "exo": "Exodus",
    "lev": "Leviticus",
    "num": "Numbers",
    "deu": "Deuteronomy",
    # Nevi'im
    "jos": "Joshua",
    "jdg": "Judges",
    "jud": "Judges",
    "1sa": "I Samuel",
    "2sa": "II Samuel",
    "1ki": "I Kings",
    "2ki": "II Kings",
    "isa": "Isaiah",
    "jer": "Jeremiah",
    "lam": "Lamentations",
    "eze": "Ezekiel",
    # The Twelve
    "hos": "Hosea",
    "joe": "Joel",
    "amo": "Amos",
    "oba": "Obadiah",
    "jon": "Jonah",
    "mic": "Micah",
    "nah": "Nahum",
    "hab": "Habakkuk",
    "zep": "Zephaniah",
    "hag": "Haggai",
    "zec": "Zechariah",
    "mal": "Malachi",
    # Ketuvim
    "psa": "Psalms",
    "ps": "Psalms",
    "pro": "Proverbs",
    "job": "Job",
    "sng": "Song of Songs",
    "sos": "Song of Songs",
    "rut": "Ruth",
    "ecc": "Ecclesiastes",
    "est": "Esther",
    "dan": "Daniel",
    "ezr": "Ezra",
    "neh": "Nehemiah",
    "1ch": "I Chronicles",
    "2ch": "II Chronicles",
}


# -----------------------------
# Data models
# -----------------------------
@dataclass(frozen=True)
class Silence:
    start: float
    end: float
    duration: float

    @property
    def mid(self) -> float:
        return (self.start + self.end) / 2.0


@dataclass(frozen=True)
class Segment:
    verse: int
    start: float
    end: float
    he_text: str

    @property
    def duration(self) -> float:
        return max(0.0, self.end - self.start)


# -----------------------------
# Utilities
# -----------------------------

def eprint(*args: object) -> None:
    print(*args, file=sys.stderr)


def check_dependency(cmd: str) -> None:
    try:
        subprocess.run([cmd, "-version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    except FileNotFoundError as ex:
        raise SystemExit(
            f"Missing dependency: '{cmd}' was not found on your PATH.\n"
            f"Install FFmpeg (which includes {cmd}) and try again."
        ) from ex
    except subprocess.CalledProcessError as ex:
        raise SystemExit(
            f"Dependency check failed for '{cmd}'. Is FFmpeg installed correctly?"
        ) from ex


def run(cmd: Sequence[str], *, capture: bool = False) -> subprocess.CompletedProcess:
    if capture:
        return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return subprocess.run(cmd)


# -----------------------------
# Parsing input filename
# -----------------------------

def parse_book_and_chapter_from_path(mp3_path: Path) -> Tuple[str, int, str]:
    """Return (book_code, chapter_int, base_stem).

    base_stem is the filename without extension (e.g. exo-12).
    """
    stem = mp3_path.stem

    # Accept: exo-12, exo_12, 1sa-3, 2ch-10, etc.
    m = re.match(r"(?i)^([1-3]?[a-z]{2,})[-_](\d{1,3})$", stem)
    if not m:
        raise SystemExit(
            "Input filename must encode book+chapter like 'exo-12.mp3' (book-chapter).\n"
            f"Got: {mp3_path.name}"
        )

    book_code = m.group(1).lower()
    chapter = int(m.group(2))
    return book_code, chapter, stem


def book_code_to_sefaria_title(book_code: str) -> str:
    if book_code in BOOK_CODE_TO_TITLE:
        return BOOK_CODE_TO_TITLE[book_code]

    # Some common alternate codes
    normalized = book_code.lower().replace(" ", "")
    aliases = {
        "dt": "deu",
        "deut": "deu",
        "ex": "exo",
        "ge": "gen",
        "gn": "gen",
        "lv": "lev",
        "nu": "num",
        "nm": "num",
        "psalm": "psa",
        "psalms": "psa",
        "prov": "pro",
        "qohelet": "ecc",
        "eccl": "ecc",
        "cant": "sng",
        "song": "sng",
        "1sam": "1sa",
        "2sam": "2sa",
        "1kgs": "1ki",
        "2kgs": "2ki",
        "1chr": "1ch",
        "2chr": "2ch",
    }
    if normalized in aliases:
        return BOOK_CODE_TO_TITLE[aliases[normalized]]

    known = ", ".join(sorted(BOOK_CODE_TO_TITLE.keys()))
    raise SystemExit(
        f"Unknown book code '{book_code}'.\n"
        f"Add it to BOOK_CODE_TO_TITLE in the script, or rename the file to use a known code.\n"
        f"Known codes: {known}"
    )


# -----------------------------
# Hebrew text retrieval (Sefaria)
# -----------------------------

def sefaria_texts_v1_url(tref: str, *, context: int = 0, commentary: int = 0, pad: int = 0) -> str:
    # Spaces should be replaced with underscores for URL use.
    tref_url = tref.replace(" ", "_")
    qs = urlencode({"context": str(context), "commentary": str(commentary), "pad": str(pad)})
    return f"https://www.sefaria.org/api/texts/{tref_url}?{qs}"


def fetch_json(url: str, *, timeout_s: float = 20.0) -> dict:
    req = Request(url, headers={"User-Agent": "tanakh_splitter/1.0"})
    try:
        with urlopen(req, timeout=timeout_s) as resp:
            body = resp.read().decode("utf-8")
        return json.loads(body)
    except HTTPError as ex:
        raise RuntimeError(f"HTTP error {ex.code} while fetching {url}") from ex
    except URLError as ex:
        raise RuntimeError(f"Network error while fetching {url}: {ex}") from ex
    except json.JSONDecodeError as ex:
        raise RuntimeError(f"Invalid JSON from {url}") from ex


def get_hebrew_verses_from_sefaria(book_title: str, chapter: int, cache_dir: Path) -> Tuple[List[str], str]:
    """Returns (verses_he, source_url).

    Caches the raw JSON response in cache_dir.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Sefaria URL refs typically separate sections with dots (e.g. Kohelet.5, Exodus.12).
    # Titles with spaces use underscores in URLs (handled in `sefaria_texts_v1_url`).
    tref = f"{book_title}.{chapter}"
    url = sefaria_texts_v1_url(tref, context=0, commentary=0, pad=0)

    cache_key = f"sefaria_v1__{book_title.replace(' ', '_')}__{chapter}.json"
    cache_path = cache_dir / cache_key

    if cache_path.exists():
        try:
            data = json.loads(cache_path.read_text(encoding="utf-8"))
            verses = data.get("he")
            if isinstance(verses, list) and verses:
                return [str(v) for v in verses], url
        except Exception:
            # Fall through to re-fetch
            pass

    data = fetch_json(url)
    cache_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    verses = data.get("he")
    if not isinstance(verses, list) or not verses:
        raise RuntimeError(
            f"Sefaria response for '{tref}' did not include a Hebrew verse list in field 'he'.\n"
            f"URL: {url}"
        )

    return [str(v) for v in verses], url


# -----------------------------
# Audio analysis
# -----------------------------

def ffprobe_duration_seconds(audio_path: Path) -> float:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(audio_path),
    ]
    proc = run(cmd, capture=True)
    if proc.returncode != 0:
        raise RuntimeError(f"ffprobe failed:\n{proc.stderr}")

    try:
        return float(proc.stdout.strip())
    except ValueError as ex:
        raise RuntimeError(f"Could not parse duration from ffprobe output: {proc.stdout!r}") from ex


_SILENCE_START_RE = re.compile(r"silence_start:\s*(?P<t>[-0-9.]+)")
_SILENCE_END_RE = re.compile(r"silence_end:\s*(?P<t>[-0-9.]+)\s*\|\s*silence_duration:\s*(?P<d>[-0-9.]+)")


def detect_silences(
    audio_path: Path,
    *,
    noise_db: float,
    detect_d: float,
) -> List[Silence]:
    """Run FFmpeg silencedetect and parse silences.

    noise_db: threshold, e.g. -35 (in dB)
    detect_d: minimum silence duration to log (seconds)

    Returns list of Silence(start,end,duration).
    """

    filt = f"silencedetect=noise={noise_db}dB:d={detect_d}"
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-nostats",
        "-loglevel",
        "info",
        "-i",
        str(audio_path),
        "-af",
        filt,
        "-f",
        "null",
        "-",
    ]
    proc = run(cmd, capture=True)

    # ffmpeg returns 0 even though output is null; but if file can't be read it will be nonzero.
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg silencedetect failed:\n{proc.stderr}")

    silences: List[Silence] = []
    current_start: Optional[float] = None

    for line in proc.stderr.splitlines():
        m_start = _SILENCE_START_RE.search(line)
        if m_start:
            try:
                current_start = float(m_start.group("t"))
            except ValueError:
                current_start = None
            continue

        m_end = _SILENCE_END_RE.search(line)
        if m_end and current_start is not None:
            try:
                end_t = float(m_end.group("t"))
                dur = float(m_end.group("d"))
            except ValueError:
                current_start = None
                continue

            silences.append(Silence(start=current_start, end=end_t, duration=dur))
            current_start = None

    return silences


# -----------------------------
# Choosing verse boundaries
# -----------------------------

def _boundary_cost(expected_t: float, cand: Silence) -> float:
    # Balance: closeness to expected location AND preference for longer silences.
    # Larger duration should reduce cost a bit.
    time_err = abs(cand.mid - expected_t)
    dur_bonus = 0.15 * cand.duration
    return time_err - dur_bonus


def choose_boundaries_dp(
    candidates: List[Silence],
    *,
    total_duration: float,
    verse_count: int,
    min_gap_s: float,
) -> List[Silence]:
    """Pick exactly verse_count-1 boundaries from candidates, in chronological order.

    Uses a monotone DP aligned to expected evenly-spaced boundary times.
    """

    n_boundaries = verse_count - 1
    if n_boundaries <= 0:
        return []

    if len(candidates) < n_boundaries:
        raise ValueError("Not enough candidate silences")

    cands = sorted(candidates, key=lambda s: s.mid)
    times = [c.mid for c in cands]

    expected = [((i + 1) * total_duration / verse_count) for i in range(n_boundaries)]

    m = len(cands)
    INF = 1e18

    back: List[List[int]] = [[-1] * m for _ in range(n_boundaries)]

    dp_prev = [_boundary_cost(expected[0], cands[j]) for j in range(m)]

    for i in range(1, n_boundaries):
        dp_curr = [INF] * m

        best_cost = INF
        best_idx = -1
        k = 0
        for j in range(m):
            # Ensure strict monotonicity and minimum spacing between chosen boundaries.
            # Update best_cost using dp_prev[k] while candidate k is far enough behind j.
            while k < j and times[k] <= times[j] - min_gap_s:
                if dp_prev[k] < best_cost:
                    best_cost = dp_prev[k]
                    best_idx = k
                k += 1

            if best_idx == -1:
                continue

            dp_curr[j] = best_cost + _boundary_cost(expected[i], cands[j])
            back[i][j] = best_idx

        dp_prev = dp_curr

    # pick best last boundary
    j_end = min(range(m), key=lambda j: dp_prev[j])
    if dp_prev[j_end] >= INF / 2:
        raise ValueError("DP could not find a valid boundary chain")

    idxs = [j_end]
    for i in range(n_boundaries - 1, 0, -1):
        j_end = back[i][j_end]
        if j_end < 0:
            raise ValueError("DP backtracking failed")
        idxs.append(j_end)

    idxs.reverse()
    return [cands[i] for i in idxs]


def select_verse_boundary_silences(
    audio_path: Path,
    *,
    total_duration: float,
    verse_count: int,
    noise_db_candidates: Sequence[float],
    detect_d: float,
    min_boundary_silence_s: float,
    ignore_edge_silence_s: float,
) -> Tuple[List[Silence], Dict[str, float]]:
    """Detect silences and select exactly verse_count-1 that are most likely verse boundaries.

    Returns (selected_silences, params_used).
    """

    target = max(0, verse_count - 1)

    best: Optional[List[Silence]] = None
    best_params: Optional[Dict[str, float]] = None
    best_score = float("inf")

    for noise_db in noise_db_candidates:
        silences = detect_silences(audio_path, noise_db=noise_db, detect_d=detect_d)

        # Remove leading/trailing silence (often room tone).
        silences = [
            s
            for s in silences
            if s.end > ignore_edge_silence_s and s.start < (total_duration - ignore_edge_silence_s)
        ]

        # Filter out tiny silences (breaths, micro-pauses).
        candidates = [s for s in silences if s.duration >= min_boundary_silence_s]

        if target == 0:
            return [], {"noise_db": noise_db, "detect_d": detect_d}

        if not candidates:
            continue

        # We want at least enough candidates to pick from.
        # But too many can also be noisy, so score based on how close count is to target.
        # Lower is better.
        count = len(candidates)
        if count < target:
            # Not enough boundaries; still consider, but penalize heavily.
            score = (target - count) * 1000.0
        else:
            score = (count - target) * 5.0

        # Prefer settings that detect longer silences on average.
        avg_dur = sum(s.duration for s in candidates) / max(1, len(candidates))
        score -= avg_dur

        if score < best_score:
            best_score = score
            best = candidates
            best_params = {"noise_db": float(noise_db), "detect_d": float(detect_d)}

        # If we found a very close match, break early.
        if count == target:
            break

    if best is None or best_params is None:
        raise RuntimeError(
            "Could not detect any suitable silences. "
            "Try setting --noise-db closer to your audio noise floor, or lowering --min-boundary-silence."
        )

    # Now, from best candidates, choose exactly target silences.
    if target == 0:
        return [], best_params

    avg_verse_len = total_duration / max(1, verse_count)
    # Try a couple of min_gap values, from stricter to looser.
    min_gap_trials = [max(0.10, 0.20 * avg_verse_len), max(0.05, 0.10 * avg_verse_len), 0.0]

    last_err: Optional[Exception] = None
    for min_gap in min_gap_trials:
        try:
            chosen = choose_boundaries_dp(
                best,
                total_duration=total_duration,
                verse_count=verse_count,
                min_gap_s=min_gap,
            )
            if len(chosen) != target:
                raise ValueError("Boundary selection produced wrong count")
            return chosen, {**best_params, "min_gap_s": float(min_gap), "min_boundary_silence_s": float(min_boundary_silence_s)}
        except Exception as ex:
            last_err = ex

    raise RuntimeError(
        "Detected silences, but could not select a consistent chain of verse boundaries. "
        "Try tuning --min-boundary-silence / --noise-db / --detect-d."
    ) from last_err


# -----------------------------
# Build segments and split audio
# -----------------------------

def build_segments_from_silences(
    verses_he: List[str],
    *,
    total_duration: float,
    boundary_silences: List[Silence],
    trim_silence: bool,
    start_pad_s: float,
    end_pad_s: float,
) -> List[Segment]:
    verse_count = len(verses_he)
    if verse_count == 0:
        raise ValueError("No verses")

    if len(boundary_silences) != max(0, verse_count - 1):
        raise ValueError(
            f"Expected {verse_count-1} boundary silences but got {len(boundary_silences)}"
        )

    segments: List[Segment] = []

    cursor = 0.0
    for i, silence in enumerate(boundary_silences):
        mid = silence.mid

        if trim_silence:
            # Keep a small bit of the detected silence on each side to avoid clipping speech.
            end_t = min(total_duration, max(0.0, silence.start + end_pad_s))
            next_start = min(total_duration, max(0.0, silence.end - start_pad_s))

            # If padding collapses or overlaps, fall back to midpoint.
            if next_start <= end_t:
                end_t = mid
                next_start = mid
        else:
            end_t = mid
            next_start = mid

        # Guard monotonicity
        end_t = max(cursor, min(total_duration, end_t))
        next_start = max(end_t, min(total_duration, next_start))

        segments.append(Segment(verse=i + 1, start=cursor, end=end_t, he_text=verses_he[i]))
        cursor = next_start

    # Final verse
    segments.append(Segment(verse=verse_count, start=cursor, end=total_duration, he_text=verses_he[-1]))

    return segments


def split_audio_to_segments(
    audio_path: Path,
    *,
    segments: List[Segment],
    out_dir: Path,
    out_base: str,
    mp3_quality: int,
    overwrite: bool,
    quiet: bool,
) -> List[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)

    width = max(2, len(str(len(segments))))
    out_paths: List[Path] = []

    for seg in segments:
        out_name = f"{out_base}-{seg.verse:0{width}d}.mp3"
        out_path = out_dir / out_name
        out_paths.append(out_path)

        if out_path.exists() and not overwrite:
            if not quiet:
                eprint(f"[skip] {out_path.name} already exists")
            continue

        dur = seg.duration
        # Avoid ffmpeg errors on extremely short segments.
        if dur < 0.02:
            if not quiet:
                eprint(f"[warn] Verse {seg.verse} segment too short ({dur:.3f}s). Forcing 0.02s")
            dur = 0.02

        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error" if quiet else "warning",
            "-y" if overwrite else "-n",
            "-ss",
            f"{seg.start:.3f}",
            "-t",
            f"{dur:.3f}",
            "-i",
            str(audio_path),
            "-vn",
            "-c:a",
            "libmp3lame",
            "-q:a",
            str(mp3_quality),
            str(out_path),
        ]

        proc = run(cmd, capture=True)
        if proc.returncode != 0:
            raise RuntimeError(
                f"ffmpeg failed while writing {out_path.name}\n"
                f"STDERR:\n{proc.stderr}"
            )

        if not quiet:
            eprint(f"[ok] {out_path.name}  ({seg.start:.2f} → {seg.end:.2f}, {seg.duration:.2f}s)")

    return out_paths


def write_manifest(
    *,
    out_dir: Path,
    out_base: str,
    input_audio: Path,
    book_code: str,
    book_title: str,
    chapter: int,
    total_duration: float,
    text_source_url: str,
    boundary_params: Dict[str, float],
    segments: List[Segment],
) -> Path:
    width = max(2, len(str(len(segments))))

    manifest = {
        "input_audio": str(input_audio),
        "book_code": book_code,
        "book_title": book_title,
        "chapter": chapter,
        "total_duration_seconds": total_duration,
        "verse_count": len(segments),
        "text_source": {
            "provider": "sefaria",
            "api": "texts_v1",
            "url": text_source_url,
        },
        "silence_detection": boundary_params,
        "segments": [
            {
                "verse": s.verse,
                "start": round(s.start, 6),
                "end": round(s.end, 6),
                "duration": round(s.duration, 6),
                "file": f"{out_base}-{s.verse:0{width}d}.mp3",
                "he": s.he_text,
            }
            for s in segments
        ],
    }

    out_path = out_dir / f"{out_base}-manifest.json"
    out_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path


# -----------------------------
# CLI
# -----------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Split a Hebrew Bible chapter MP3 into verse MP3s using silence detection.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    p.add_argument("chapter_mp3", type=Path, help="Path to chapter MP3 named like exo-12.mp3")
    p.add_argument("output_dir", type=Path, help="Folder to write verse MP3 files")

    p.add_argument(
        "--cache-dir",
        type=Path,
        default=Path.home() / ".cache" / "tanakh_splitter",
        help="Where to cache downloaded Hebrew text JSON",
    )

    # Silence detection tuning
    p.add_argument(
        "--noise-db",
        type=float,
        default=None,
        help=(
            "Override silence threshold in dB (e.g. -35). "
            "If omitted, the script tries a small set of thresholds automatically."
        ),
    )
    p.add_argument(
        "--detect-d",
        type=float,
        default=0.05,
        help="Minimum silence duration (seconds) for FFmpeg to log a silence",
    )
    p.add_argument(
        "--min-boundary-silence",
        type=float,
        default=0.10,
        help="Ignore detected silences shorter than this when choosing verse boundaries",
    )
    p.add_argument(
        "--ignore-edge-silence",
        type=float,
        default=0.20,
        help="Ignore silences very near the start/end of the file (seconds)",
    )

    # Cutting behavior
    p.add_argument(
        "--trim-silence",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Trim using silence start/end (with padding) instead of cutting at silence midpoints",
    )
    p.add_argument(
        "--start-pad",
        type=float,
        default=0.03,
        help="Padding (seconds) before the detected silence end for the next verse start",
    )
    p.add_argument(
        "--end-pad",
        type=float,
        default=0.03,
        help="Padding (seconds) after the detected silence start for the previous verse end",
    )

    # Output encoding
    p.add_argument(
        "--mp3-quality",
        type=int,
        default=2,
        help="libmp3lame VBR quality (0=best, 9=worst)",
    )
    p.add_argument(
        "--overwrite",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Overwrite existing output files",
    )
    p.add_argument(
        "--quiet",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Reduce console output",
    )

    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)

    check_dependency("ffmpeg")
    check_dependency("ffprobe")

    audio_path: Path = args.chapter_mp3
    out_dir: Path = args.output_dir

    if not audio_path.exists():
        raise SystemExit(f"Input file not found: {audio_path}")

    book_code, chapter, out_base = parse_book_and_chapter_from_path(audio_path)
    book_title = book_code_to_sefaria_title(book_code)

    if not args.quiet:
        eprint(f"Book: {book_title}   Chapter: {chapter}   (from '{audio_path.name}')")

    # Hebrew verses
    try:
        verses_he, text_url = get_hebrew_verses_from_sefaria(book_title, chapter, args.cache_dir)
    except Exception as ex:
        raise SystemExit(
            "Failed to fetch Hebrew text for this chapter.\n"
            "You need internet access on first run, or a valid cache in --cache-dir.\n\n"
            f"Error: {ex}"
        ) from ex

    verse_count = len(verses_he)
    if not args.quiet:
        eprint(f"Hebrew verses: {verse_count} (source: Sefaria)")

    # Duration
    total_duration = ffprobe_duration_seconds(audio_path)
    if not args.quiet:
        eprint(f"Audio duration: {total_duration:.2f} seconds")

    # Silence detection & boundary selection
    if args.noise_db is None:
        noise_candidates = [-40.0, -35.0, -30.0, -25.0, -20.0]
    else:
        noise_candidates = [float(args.noise_db)]

    boundary_silences, boundary_params = select_verse_boundary_silences(
        audio_path,
        total_duration=total_duration,
        verse_count=verse_count,
        noise_db_candidates=noise_candidates,
        detect_d=float(args.detect_d),
        min_boundary_silence_s=float(args.min_boundary_silence),
        ignore_edge_silence_s=float(args.ignore_edge_silence),
    )

    if not args.quiet:
        eprint(
            f"Selected {len(boundary_silences)} verse-boundary silences "
            f"(noise_db={boundary_params.get('noise_db')}, detect_d={boundary_params.get('detect_d')}, "
            f"min_boundary_silence={boundary_params.get('min_boundary_silence_s')}, min_gap={boundary_params.get('min_gap_s')})"
        )

    # Build segments
    segments = build_segments_from_silences(
        verses_he,
        total_duration=total_duration,
        boundary_silences=boundary_silences,
        trim_silence=bool(args.trim_silence),
        start_pad_s=float(args.start_pad),
        end_pad_s=float(args.end_pad),
    )

    # Split audio
    split_audio_to_segments(
        audio_path,
        segments=segments,
        out_dir=out_dir,
        out_base=out_base,
        mp3_quality=int(args.mp3_quality),
        overwrite=bool(args.overwrite),
        quiet=bool(args.quiet),
    )

    # Manifest
    manifest_path = write_manifest(
        out_dir=out_dir,
        out_base=out_base,
        input_audio=audio_path,
        book_code=book_code,
        book_title=book_title,
        chapter=chapter,
        total_duration=total_duration,
        text_source_url=text_url,
        boundary_params=boundary_params,
        segments=segments,
    )

    if not args.quiet:
        eprint(f"Manifest written: {manifest_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())