#!/usr/bin/env python3
"""
Word & Morpheme Frequency Counter
=================================

Given one or more text/markdown files, this script produces three CSVs:
  1) word_counts.csv     -> columns: word, count, isStopword
  2) lemma_counts.csv    -> columns: lemma, count, isStopword
  3) morpheme_counts.csv -> columns: morpheme, count, isStopword

Definitions
-----------
- "Ignore punctuations and capitalizations": everything is processed in lowercase, and
  punctuation is dropped. Apostrophes *inside* words are preserved (e.g., "don't").
- #1 (word): tokens are literal words as they appear (lowercased), including internal
  apostrophes like "don't", "rock'n'roll".
- #2 (lemma): lemmatization is attempted with spaCy's English model (en_core_web_sm).
  If spaCy/model is unavailable, a simple built-in rule-based fallback is used.
- #3 (morpheme): a light-weight heuristic English morpheme segmenter splits common
  prefixes/suffixes and clitics (e.g., re- / -ing / -ed / -tion, and n't/'re/'s...).
  This is intentionally simple and language-specific; it won't be perfect but is
  reasonably useful for quick corpus analyses.

Quickstart
----------
1) (Recommended) Install spaCy + English model for higher-quality lemmas:

    pip install spacy
    python -m spacy download en_core_web_sm

2) Run the script:

    python word_morph_counter.py --files "doc1.md,notes.txt" --output-dir out

3) Outputs will appear in the chosen directory:
   out/word_counts.csv, out/lemma_counts.csv, out/morpheme_counts.csv

Notes
-----
- Markdown is lightly cleaned (code fences, inline code, links) prior to processing.
- Stopwords default to spaCy's English list; if spaCy isn't available, a bundled
  English stopword list is used.
- The script tries to be robust and dependency-light. If spaCy isn't installed, it
  will still produce output (with a simple lemmatizer fallback).
"""
from __future__ import annotations

import argparse
import csv
import re
from collections import Counter
from pathlib import Path
from typing import Iterable, List, Tuple, Optional, Dict, Set

# ------------------------------
# Stopwords (fallback set)
# ------------------------------
# A compact English stopword list (subset of NLTK/spaCy) used if spaCy isn't available.
# All terms should be lowercase.
FALLBACK_STOPWORDS: Set[str] = {
    'a','about','above','after','again','against','all','am','an','and','any','are','aren\'t','as','at',
    'be','because','been','before','being','below','between','both','but','by','can\'t','cannot','could',
    'couldn\'t','did','didn\'t','do','does','doesn\'t','doing','don\'t','down','during','each','few','for',
    'from','further','had','hadn\'t','has','hasn\'t','have','haven\'t','having','he','he\'d','he\'ll','he\'s',
    'her','here','here\'s','hers','herself','him','himself','his','how','how\'s','i','i\'d','i\'ll','i\'m',
    'i\'ve','if','in','into','is','isn\'t','it','it\'s','its','itself','let\'s','me','more','most','mustn\'t',
    'my','myself','no','nor','not','of','off','on','once','only','or','other','ought','our','ours','ourselves',
    'out','over','own','same','shan\'t','she','she\'d','she\'ll','she\'s','should','shouldn\'t','so','some',
    'such','than','that','that\'s','the','their','theirs','them','themselves','then','there','there\'s','these',
    'they','they\'d','they\'ll','they\'re','they\'ve','this','those','through','to','too','under','until','up',
    'very','was','wasn\'t','we','we\'d','we\'ll','we\'re','we\'ve','were','weren\'t','what','what\'s','when',
    'when\'s','where','where\'s','which','while','who','who\'s','whom','why','why\'s','with','won\'t','would',
    'wouldn\'t','you','you\'d','you\'ll','you\'re','you\'ve','your','yours','yourself','yourselves'
}

# ------------------------------
# Markdown cleaning
# ------------------------------
MD_FENCE_RE = re.compile(r"```[\s\S]*?```", re.MULTILINE)
MD_INLINE_CODE_RE = re.compile(r"`[^`]*`")
MD_LINK_RE = re.compile(r"\[([^\]]+)\]\([^\)]+\)")  # [text](url) -> text
MD_IMAGE_RE = re.compile(r"!\[[^\]]*\]\([^\)]+\)")  # drop entirely
HTML_TAG_RE = re.compile(r"<[^>]+>")

# ------------------------------
# Tokenization (retain apostrophes inside words)
# ------------------------------
WORD_RE = re.compile(r"[a-z0-9]+(?:'[a-z0-9]+)*")

# ------------------------------
# Contraction clitics for morpheme segmentation
# ------------------------------
CLITIC_SUFFIXES = ("n't", "'re", "'ve", "'ll", "'d", "'m", "'s")

# Common English prefixes/suffixes for a simple segmenter (approximate!)
PREFIXES = [
    'anti','auto','bi','co','contra','counter','de','dis','em','en','ex','extra','hetero','homo','hyper',
    'il','im','in','inter','intra','ir','macro','micro','mid','mini','mis','mono','multi','non','over',
    'pan','post','pre','proto','pseudo','re','semi','sub','super','tele','thermo','trans','tri','ultra',
    'un','under','uni','pro'
]
# Sort longest-first to peel the biggest match
PREFIXES.sort(key=len, reverse=True)

SUFFIXES = [
    'ization','ational','fulness','ousness','iveness','tional','biliti','lessli','entli','ation','alism',
    'aliti','ousli','iviti','fulli','enci','anci','izer','ator','ally','ably','ably','less','ment','ship',
    'hood','dom','tion','sion','xion','ingly','edly','ingly','ingly','edly','ing','ings','ed','er','est',
    'able','ible','ism','ist','ity','ive','ous','ize','ise','ment','ness','al','en','y','ward','wise','s','es'
]
SUFFIXES = sorted(set(SUFFIXES), key=len, reverse=True)

# ------------------------------
# Utility
# ------------------------------

def strip_markdown(text: str) -> str:
    """Lightly strip markdown and HTML to reduce noise.
    We remove code fences, inline code, images entirely, and replace links with their text.
    """
    text = MD_FENCE_RE.sub("\n", text)
    text = MD_INLINE_CODE_RE.sub(" ", text)
    text = MD_IMAGE_RE.sub(" ", text)
    text = MD_LINK_RE.sub(r"\1", text)
    text = HTML_TAG_RE.sub(" ", text)
    return text


def read_files(paths: List[Path]) -> str:
    chunks: List[str] = []
    for p in paths:
        try:
            chunks.append(p.read_text(encoding="utf-8", errors="ignore"))
        except Exception as e:
            raise SystemExit(f"Failed to read {p}: {e}")
    return "\n\n".join(chunks)


def tokenize_words(text: str) -> List[str]:
    text = text.lower()
    # Replace dashes/underscores with spaces to avoid gluing words
    text = text.replace("-", " ").replace("_", " ")
    return WORD_RE.findall(text)


# ------------------------------
# Lemmatization
# ------------------------------

def _load_spacy() -> Tuple[Optional[object], Set[str]]:
    """Try to load spaCy English model and stopwords. Return (nlp, stopwords_set)."""
    try:
        import spacy  # type: ignore
        try:
            nlp = spacy.load("en_core_web_sm")
        except Exception:
            # Try to download on the fly; if it fails, fall back.
            try:
                from spacy.cli import download  # type: ignore
                download("en_core_web_sm")
                nlp = spacy.load("en_core_web_sm")
            except Exception:
                nlp = None
        # spaCy STOP_WORDS is available without model
        try:
            from spacy.lang.en.stop_words import STOP_WORDS as SPACY_STOPWORDS  # type: ignore
            sw = set(map(str.lower, SPACY_STOPWORDS))
        except Exception:
            sw = set(FALLBACK_STOPWORDS)
        return nlp, sw
    except Exception:
        # spaCy not installed
        return None, set(FALLBACK_STOPWORDS)


def simple_rule_lemma(word: str) -> str:
    """A very small heuristic English lemmatizer (fallback if spaCy isn't available).
    Handles a few common inflections; NOT linguistically complete.
    """
    w = word
    # Irregular quick fixes
    IRREG = {
        'went': 'go', 'gone': 'go', 'better': 'good', 'best': 'good', 'worse': 'bad', 'worst': 'bad',
        'did': 'do', 'done': 'do', 'had': 'have', 'has': 'have', 'does': 'do', 'aren\'t': 'be', 'isn\'t': 'be',
        'was': 'be', 'were': 'be', 'been': 'be', 'am': 'be', "i\'m": 'be', "you\'re": 'be', "we\'re": 'be',
        "they\'re": 'be', "it\'s": 'it', "don\'t": 'do', "can\'t": 'can', "won\'t": 'will'
    }
    if w in IRREG:
        return IRREG[w]

    # Plurals
    if len(w) > 4 and w.endswith('ies') and not w.endswith('eies'):
        return w[:-3] + 'y'
    if len(w) > 3 and (w.endswith('ses') or w.endswith('xes') or w.endswith('zes')):
        return w[:-2]  # classes -> class, boxes -> box
    if len(w) > 2 and w.endswith('s') and not w.endswith('ss'):
        return w[:-1]

    # Past / continuous
    if len(w) > 4 and w.endswith('ied'):
        return w[:-3] + 'y'
    if len(w) > 3 and w.endswith('ed'):
        return w[:-2]
    if len(w) > 4 and w.endswith('ing'):
        base = w[:-3]
        # handle doubling (running -> run)
        if len(base) > 3 and base[-1] == base[-2]:
            base = base[:-1]
        return base

    return w


def lemmatize_counts(text: str, nlp: Optional[object]) -> Counter:
    """Return lemma frequency Counter for the given text.
    Uses spaCy if available; otherwise a simple rule-based fallback.
    """
    if nlp is not None:
        # type: ignore[attr-defined]
        doc = nlp(text)  # type: ignore
        lemmas: List[str] = []
        for tok in doc:  # type: ignore[attr-defined]
            if tok.is_space or tok.is_punct:
                continue
            t = tok.text.lower()
            # Ignore standalone punctuation/nums
            if not WORD_RE.fullmatch(t):
                continue
            # Prefer lemma_ if available; ensure lowercase
            lem = tok.lemma_.lower()  # type: ignore[attr-defined]
            # Guard against PRON -> -PRON- (older spaCy versions)
            if lem == '-pron-':
                lem = t
            lemmas.append(lem)
        return Counter(lemmas)
    else:
        # Fallback: tokenization + simple rules
        toks = tokenize_words(text)
        return Counter(simple_rule_lemma(t) for t in toks)


# ------------------------------
# Morpheme segmentation (simple heuristic)
# ------------------------------

def split_clitic(word: str) -> Tuple[str, Optional[str]]:
    for c in CLITIC_SUFFIXES:
        if word.endswith(c) and len(word) > len(c) + 1:
            return word[:-len(c)], c
    return word, None


def segment_morphemes(word: str) -> List[str]:
    """Segment a word into approximate morphemes.
    Strategy:
      1) Separate common English clitic suffixes (n't, 're, 've, 'll, 'd, 'm, 's).
      2) Peel off ONE longest known prefix (if any) when remainder >= 3.
      3) Peel off multiple known suffixes (long->short) while remainder >= 3.
      4) Return [prefix?, root, suffixes..., clitic?]
    """
    w = word
    pieces: List[str] = []

    # Step 1: clitic
    base, clitic = split_clitic(w)

    # Step 2: prefix (at most one, longest)
    root = base
    for pref in PREFIXES:
        if root.startswith(pref) and len(root) - len(pref) >= 3:
            pieces.append(pref)
            root = root[len(pref):]
            break

    # Step 3: suffixes (greedy, longest-first, multiple)
    suffixes_found: List[str] = []
    searching = True
    while searching:
        searching = False
        for suff in SUFFIXES:
            if root.endswith(suff) and len(root) - len(suff) >= 3:
                suffixes_found.append(suff)
                root = root[:-len(suff)]
                searching = True
                break

    # If root collapsed too far, back off: ensure at least 2 chars in root
    if len(root) < 2 and pieces:
        # Put the prefix back if root too short
        last_pref = pieces.pop()
        root = last_pref + root

    # Assemble order: prefix(es) + root + suffix(es)
    out = pieces + [root]
    if suffixes_found:
        out.extend(reversed(suffixes_found))  # maintain natural order from inner->outer

    if clitic:
        out.append(clitic)

    # Sanity: filter empties and very short scraps (keep 1-letter only if it's "i" or "a")
    cleaned: List[str] = []
    for m in out:
        if not m:
            continue
        if len(m) == 1 and m not in {'i', 'a'}:
            continue
        cleaned.append(m)
    return cleaned if cleaned else [word]


def count_morphemes(tokens: Iterable[str]) -> Counter:
    c = Counter()
    for t in tokens:
        for m in segment_morphemes(t):
            c[m] += 1
    return c


# ------------------------------
# CSV helpers
# ------------------------------

def write_csv_counts(path: Path, counts: Counter, stopwords: Set[str], header: Tuple[str, str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    col_term, col_count, col_stop = header
    rows = ((term, cnt, term in stopwords) for term, cnt in counts.items())
    # sort by count desc, then alpha
    sorted_rows = sorted(rows, key=lambda r: (-r[1], r[0]))
    with path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([col_term, col_count, col_stop])
        for term, cnt, is_sw in sorted_rows:
            writer.writerow([term, cnt, str(is_sw)])


# ------------------------------
# Main
# ------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Count word, lemma, and morpheme frequencies from text/markdown files.")
    p.add_argument('--files', required=True, help='Comma-separated list of input files (e.g., "a.txt,b.md")')
    p.add_argument('--output-dir', default='out', help='Directory to write CSV outputs (default: out)')
    p.add_argument('--no-strip-markdown', action='store_true', help='Do not strip markdown formatting (default: strip)')
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # Parse and validate input files
    file_paths: List[Path] = [Path(s.strip()) for s in args.files.split(',') if s.strip()]
    if not file_paths:
        raise SystemExit('No input files provided.')
    for p in file_paths:
        if not p.exists():
            raise SystemExit(f"Input file not found: {p}")

    raw_text = read_files(file_paths)
    text = raw_text if args.no_strip_markdown else strip_markdown(raw_text)

    # Load spaCy (optional) and stopwords
    nlp, stopwords = _load_spacy()

    # 1) WORD COUNTS (regex tokenization preserves inner apostrophes)
    tokens = tokenize_words(text)
    word_counts = Counter(tokens)

    # 2) LEMMA COUNTS (spaCy if available; else fallback rules)
    lemma_counts = lemmatize_counts(text.lower(), nlp)

    # 3) MORPHEME COUNTS (heuristic)
    morpheme_counts = count_morphemes(tokens)

    # Write CSVs
    outdir = Path(args.output_dir)
    write_csv_counts(outdir / 'word_counts.csv', word_counts, stopwords, ('word', 'count', 'isStopword'))
    write_csv_counts(outdir / 'lemma_counts.csv', lemma_counts, stopwords, ('lemma', 'count', 'isStopword'))
    write_csv_counts(outdir / 'morpheme_counts.csv', morpheme_counts, stopwords, ('morpheme', 'count', 'isStopword'))

    print(f"\nWrote: {outdir / 'word_counts.csv'}")
    print(f"Wrote: {outdir / 'lemma_counts.csv'}")
    print(f"Wrote: {outdir / 'morpheme_counts.csv'}\n")
    if nlp is None:
        print("(Note) spaCy/en_core_web_sm not available; used a simple fallback lemmatizer.\n"
              "      For better lemmas, install spaCy and its English model:\n"
              "      pip install spacy && python -m spacy download en_core_web_sm")


if __name__ == '__main__':
    main()
