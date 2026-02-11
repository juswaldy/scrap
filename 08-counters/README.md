# Counters

Small, dependency-light scripts for counting things in text corpora.

## Counters in this folder

1. **`word_morph_counter.py`** — word, lemma, and (heuristic) morpheme frequency counter.

---

## `word_morph_counter.py`

Given one or more text/markdown files, this script produces **three CSV files**:

- `word_counts.csv` — columns: `word,count,isStopword`
- `lemma_counts.csv` — columns: `lemma,count,isStopword`
- `morpheme_counts.csv` — columns: `morpheme,count,isStopword`

### What it counts

- **Word**: literal tokens as they appear (lowercased). Punctuation is dropped, but apostrophes **inside** words are preserved (e.g., `don't`, `rock'n'roll`).
- **Lemma**: uses spaCy (`en_core_web_sm`) when available; otherwise falls back to a small built-in rule-based lemmatizer.
- **Morpheme**: a simple English-specific heuristic segmenter that splits common prefixes/suffixes and clitics (e.g., `re-`, `-ing`, `-tion`, `n't`, `'re`, `'s`). This is approximate by design.

### Requirements

- Python 3
- Optional (recommended for better lemmas/stopwords):

```bash
pip install spacy
python -m spacy download en_core_web_sm
```

### Usage

```bash
python word_morph_counter.py --files "doc1.md,notes.txt" --output-dir out
```

Outputs:

- `out/word_counts.csv`
- `out/lemma_counts.csv`
- `out/morpheme_counts.csv`

### Options

- `--files` (required): comma-separated list of input files
- `--output-dir`: where to write CSVs (default: `out`)
- `--no-strip-markdown`: keep markdown formatting instead of stripping code fences/inline code/links

### Notes

- Markdown is lightly cleaned (code fences, inline code, links) by default to reduce noise.
- Stopwords come from spaCy when available; otherwise a bundled fallback list is used.
