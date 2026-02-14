# tanakh_splitter.py

Split a Tanakh (Hebrew Bible) chapter MP3 into per-verse MP3 files.

## Core features
- Reads the book+chapter from the input filename (e.g. exo-12.mp3 -> Exodus 12)
- Downloads Hebrew verse text for that chapter (default: Sefaria Texts API)
- Detects silence boundaries in the audio using FFmpeg's `silencedetect`
- Chooses exactly (verse_count - 1) boundaries and splits the audio
- Writes verse MP3s like: exo-12-01.mp3 ... exo-12-51.mp3
- Writes a manifest JSON with verse timings + Hebrew text

## Minimum parameters
  1) path to chapter mp3 (named book-chapter.mp3)
  2) output folder

Example
  python tanakh_splitter.py ./exo-12.mp3 ./out

## Requirements
- Python 3.9+
- FFmpeg + ffprobe available on PATH
- Internet access on first run (for Hebrew text fetch) OR a populated cache

## Notes
- This script is designed for chapter recordings with an audible pause between verses.
- Silence detection parameters vary by recording. The script auto-tries multiple
  thresholds, but you can override with --noise-db and --detect-d.

## Text source
- By default we use the (deprecated but still widely supported) Sefaria Texts v1 endpoint:
    https://www.sefaria.org/api/texts/{tref}?context=0&commentary=0&pad=0
  (see Sefaria API docs for parameters).
