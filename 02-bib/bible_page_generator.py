"""
Bible Page Generator
====================

This module exposes a small set of utilities for rendering pages of
Hebrew biblical text, complete with lexical footnotes, into LaTeX.
It was inspired by the layout found in classical editions of the
Hebrew Bible where each word in the main text is marked with a
numbered footnote and the corresponding lexical gloss appears in a
separate apparatus at the bottom of the page.  The goal of this
module is not to provide a one‑size‑fits‑all typesetting solution,
but rather a flexible framework that can be adapted to a variety of
layouts and data sources.

The core workflow looks like this:

1.  **Parse a source text** (for example, the `wlc` or `bhs` XML files
    maintained by the Open Scriptures project) and break it down into
    chapters, verses and words.  Each `Word` retains its Hebrew
    surface form along with any associated lemma and morphology data.

2.  **Look up lexical information** for each lemma.  By default the
    parser tries to extract a Strong’s number from the lemma and
    resolve it in a Strong’s dictionary.  The Open Scriptures
    `HebrewStrong.xml` file can be used for this purpose.  It
    contains concise English definitions and parts of speech that are
    sufficiently compact for use in a footnote apparatus.

3.  **Assign footnote numbers** to each word on a page.  A page may
    span multiple verses and even multiple chapters.  The footnote
    numbering is sequential within a page but can be seeded with an
    initial value so that numbering can carry across pages if
    desired.

4.  **Render LaTeX** for the page.  A minimal preamble is provided
    which loads `fontspec` and `bidi` so that right‑to‑left Hebrew can
    be typeset using XeLaTeX or LuaLaTeX.  The rendered page
    contains a header with the book name and page number, the
    reference range (e.g. 1:29–2:12), the main Hebrew text with
    superscript footnote markers, and a footnote apparatus at the
    bottom of the page.

The module has deliberately been written with clarity in mind.  The
main entry points are:

* `HebrewBibleParser` – parses a `wlc` style XML file into nested
  chapter/verse/word structures.
* `HebrewStrongLexicon` – loads the Strong’s dictionary and allows
  simple definition lookups by number.
* `generate_latex_page` – given a list of verses and a starting
  footnote number, produce LaTeX code for one page.

Example
-------

The following snippet shows how one might generate a LaTeX page for
Proverbs 1:29–2:12, similar to the page contained in the scanned
image provided with this task.  It assumes that `Prov.xml` from the
Open Scriptures `wlc` repository and `HebrewStrong.xml` from the
Open Scriptures Hebrew lexicon are available on disk::

    from bible_page_generator import HebrewBibleParser, HebrewStrongLexicon, generate_latex_page

    # Load the book of Proverbs from the WLC
    parser = HebrewBibleParser()
    chapters = parser.load_book('/path/to/Prov.xml')

    # Extract verses 1:29 through 2:12
    verses = []
    for chap_num in (1, 2):
        chapter = chapters.get(chap_num)
        if chap_num == 1:
            verse_range = range(29, 34)  # inclusive of 33
        else:
            verse_range = range(1, 13)   # inclusive of 12
        for v in verse_range:
            verses.append(chapter[v])

    # Load the Strong's dictionary
    lexicon = HebrewStrongLexicon()
    lexicon.load('/path/to/HebrewStrong.xml')

    # Render LaTeX
    latex = generate_latex_page(
        book_name='משלי',
        page_number=1329,
        verses=verses,
        lexicon=lexicon,
        start_footnote=90  # match the numbering in the scanned page
    )
    print(latex)

The resulting LaTeX can then be compiled with XeLaTeX to produce a
PDF page that imitates the layout of the scanned source.

Parameters
----------

The functions in this module work with a common set of parameters
derived from the structure of the scanned page.  These include:

* **reference** – the reference string for the page, e.g. "1:29–2:12".
* **book_name** – the Hebrew name of the book, e.g. "משלי".
* **page_number** – the running page number from the printed edition.
* **chapter_number** – the number of the chapter to which a verse belongs.
* **verse_number** – the verse number within a chapter.
* **verse_word_number** – the position of a word within its verse
  (first word = 1).
* **verse_word_footnumber** – the footnote number assigned to a word on the page.
* **verse_word_footnote** – the lexical gloss associated with the word.

These parameters are assembled automatically by the parsing and
rendering functions; they are documented here so that users can
understand how the underlying data is structured and, if desired,
construct alternative layouts.

Notes
-----

This module does not attempt to implement full support for every
possible intricacy of Hebrew typesetting (such as the cantillation
marks or complex ligatures).  It also does not normalise the Hebrew
text; whatever characters are present in the XML source will be
preserved in the output.  Users requiring fine control over fonts
and diacritics should adjust the LaTeX preamble in
`generate_latex_page` accordingly.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass
class Word:
    """Representation of a single Hebrew word in the source text.

    Attributes
    ----------
    text : str
        The surface form of the word as it appears in the WLC/BHS XML.
    lemma : str
        The lemma attribute from the source.  May include prefixes
        separated by slashes and an optional letter suffix.  Strong’s
        numbers can be extracted from this string.
    morph : str
        The morphological code associated with the word.
    chapter_number : int
        The chapter in which this word occurs.
    verse_number : int
        The verse in which this word occurs.
    word_number : int
        The position of the word within its verse (first word = 1).
    footnote_number : Optional[int]
        The footnote number assigned to this word on the page.
    footnote_text : Optional[str]
        The textual content of the footnote (e.g. definition).
    """

    text: str
    lemma: str
    morph: str
    chapter_number: int
    verse_number: int
    word_number: int
    footnote_number: Optional[int] = None
    footnote_text: Optional[str] = None


@dataclass
class Verse:
    """Representation of a verse consisting of a sequence of words."""

    chapter_number: int
    verse_number: int
    words: List[Word] = field(default_factory=list)


class HebrewBibleParser:
    """Parse WLC/BHS XML files into chapters, verses and words.

    The Open Scriptures Hebrew Bible (OSHB) publishes the Hebrew text
    with lemma and morphology information in OSIS XML files.  Each
    `<w>` element encodes a single word.  Words may be separated by
    `<seg type="x-maqqef">` elements to indicate a maqqef.  In this
    parser we simply ignore all non‑word tags and build a flat list
    of Word objects.

    A loaded book is represented as a dictionary mapping chapter
    numbers to a dictionary of verses.  Each verse is a :class:`Verse`
    whose ``words`` attribute contains :class:`Word` objects with
    positional information.  Prefixes (such as the common conjunction
    “ו/”, meaning “and”) are not separated from the lemma; they are
    included as they appear in the source text.  Users who wish to
    separate prefixes should perform that operation after parsing.
    """

    WORD_TAG = '{http://www.bibletechnologies.net/2003/OSIS/namespace}w'
    VERSE_TAG = '{http://www.bibletechnologies.net/2003/OSIS/namespace}verse'
    CHAPTER_TAG = '{http://www.bibletechnologies.net/2003/OSIS/namespace}chapter'

    def load_book(self, xml_path: str) -> Dict[int, Dict[int, Verse]]:
        """Load a single book from a WLC/BHS OSIS XML file.

        Parameters
        ----------
        xml_path : str
            Path to the OSIS XML file (e.g. ``Prov.xml``).

        Returns
        -------
        Dict[int, Dict[int, Verse]]
            A nested dictionary keyed by chapter and verse number.
        """
        tree = ET.parse(xml_path)
        root = tree.getroot()
        chapters: Dict[int, Dict[int, Verse]] = {}
        # Find all chapters
        for chap in root.iter(self.CHAPTER_TAG):
            # Extract numeric chapter id from osisID attribute
            osis_id = chap.get('osisID') or ''
            # osisID looks like "Prov.1"; take the part after the dot
            chap_num = int(osis_id.split('.')[-1])
            verses: Dict[int, Verse] = {}
            # Iterate through verse elements
            for verse_el in chap.iter(self.VERSE_TAG):
                v_osis = verse_el.get('osisID') or ''
                v_num_str = v_osis.split('.')[-1]
                try:
                    verse_num = int(v_num_str)
                except ValueError:
                    # Some books may have verse ranges (e.g. LXX), skip those
                    continue
                words: List[Word] = []
                word_index = 1
                for child in verse_el:
                    # Only process word elements; skip seg, note, etc.
                    if child.tag == self.WORD_TAG:
                        text = (child.text or '').strip()
                        lemma = child.get('lemma') or ''
                        morph = child.get('morph') or ''
                        words.append(
                            Word(
                                text=text,
                                lemma=lemma,
                                morph=morph,
                                chapter_number=chap_num,
                                verse_number=verse_num,
                                word_number=word_index,
                            )
                        )
                        word_index += 1
                verses[verse_num] = Verse(chapter_number=chap_num, verse_number=verse_num, words=words)
            chapters[chap_num] = verses
        return chapters


class HebrewStrongLexicon:
    """Simple loader for the Strong's Hebrew dictionary.

    The `HebrewStrong.xml` file distributed with the Open Scriptures
    `HebrewLexicon` repository contains dictionary entries keyed by
    identifiers like ``H1``, ``H2``, etc.  Each entry may contain
    several fields of interest, but for the purposes of footnotes we
    extract only the definition.  If multiple definitions are present
    (for example, when both ``<meaning>`` and ``<usage>`` appear),
    they are concatenated with a semicolon.
    """

    ENTRY_TAG = '{http://openscriptures.github.com/morphhb/namespace}entry'

    def __init__(self) -> None:
        self.entries: Dict[str, str] = {}

    def load(self, xml_path: str) -> None:
        """Load and parse the Strong’s dictionary into memory.

        Parameters
        ----------
        xml_path : str
            Path to the ``HebrewStrong.xml`` file.
        """
        tree = ET.parse(xml_path)
        root = tree.getroot()
        for entry in root.iter(self.ENTRY_TAG):
            entry_id = entry.get('id') or ''
            # Extract definition: some entries have <meaning><def> or just <meaning>
            definition_parts: List[str] = []
            # Find <meaning> child
            meaning = entry.find('{http://openscriptures.github.com/morphhb/namespace}meaning')
            if meaning is not None:
                # If <def> subelement exists, use its text; else use text of <meaning>
                def_el = meaning.find('{http://openscriptures.github.com/morphhb/namespace}def')
                if def_el is not None and (def_el.text and def_el.text.strip()):
                    definition_parts.append(def_el.text.strip())
                elif meaning.text and meaning.text.strip():
                    definition_parts.append(meaning.text.strip())
            # Optionally append usage information
            usage = entry.find('{http://openscriptures.github.com/morphhb/namespace}usage')
            if usage is not None and usage.text and usage.text.strip():
                usage_text = usage.text.strip()
                # Only take a short portion of usage; too long for footnotes
                definition_parts.append(usage_text)
            if definition_parts:
                # Join parts with a semicolon for brevity
                definition = '; '.join(definition_parts)
                self.entries[entry_id] = definition

    STRONG_NUM_RE = re.compile(r'^0*(\d+)$')

    def lookup(self, lemma: str) -> Optional[str]:
        """Return a definition string for a lemma based on its Strong’s number.

        The lemma attribute in the WLC may contain prefixes separated by
        slashes and a suffix letter (e.g. ``c/3808 a``).  This method
        extracts the first numeric sequence from the lemma and looks it
        up as ``H{number}`` in the dictionary.  If no matching entry
        exists the method returns ``None``.

        Parameters
        ----------
        lemma : str
            A lemma attribute from a WLC `<w>` element.

        Returns
        -------
        Optional[str]
            The dictionary definition if available; ``None`` otherwise.
        """
        if not self.entries:
            return None
        # Extract digits from the lemma
        m = re.search(r'(\d+)', lemma)
        if not m:
            return None
        number = m.group(1)
        strong_id = f'H{int(number)}'
        return self.entries.get(strong_id)


def assign_footnotes(verses: List[Verse], lexicon: HebrewStrongLexicon, start: int = 1) -> None:
    """Assign sequential footnote numbers and text to every word in a list of verses.

    Modifies each :class:`Word` in place by setting its ``footnote_number``
    and ``footnote_text``.  The footnote text is looked up in the
    supplied lexicon; if no definition is found the Strong’s number and
    morphological code are used as a fallback.

    Parameters
    ----------
    verses : list of :class:`Verse`
        Verses to process.
    lexicon : :class:`HebrewStrongLexicon`
        Lexicon used to translate lemmas to English glosses.
    start : int, optional
        Initial footnote number (default 1).  Footnote numbers will
        increment from this value across all words in the verses.
    """
    footnote_counter = start
    for verse in verses:
        for word in verse.words:
            definition = lexicon.lookup(word.lemma)
            # Fallback to lemma and morph if no definition
            if not definition:
                # Extract numeric part of lemma for fallback display
                m = re.search(r'(\d+)', word.lemma)
                strong_number = m.group(1) if m else word.lemma
                definition = f'Strong {strong_number}; morph {word.morph}'
            word.footnote_number = footnote_counter
            word.footnote_text = definition
            footnote_counter += 1


def compute_reference(verses: List[Verse]) -> str:
    """Compute a reference string (e.g. "1:29–2:12") for a list of verses.

    Parameters
    ----------
    verses : list of :class:`Verse`
        The verses included on the page.

    Returns
    -------
    str
        A human‑readable reference indicating the start and end
        chapter/verse (e.g. "1:29–2:12").
    """
    if not verses:
        return ''
    start_ch = verses[0].chapter_number
    start_vs = verses[0].verse_number
    end_ch = verses[-1].chapter_number
    end_vs = verses[-1].verse_number
    if start_ch == end_ch:
        return f"{start_ch}:{start_vs}–{end_vs}"
    else:
        return f"{start_ch}:{start_vs}–{end_ch}:{end_vs}"


def generate_latex_page(
    *,
    book_name: str,
    page_number: int,
    verses: List[Verse],
    lexicon: HebrewStrongLexicon,
    start_footnote: int = 1,
    hebrew_font: str = 'SBL BibLit',
) -> str:
    """Generate LaTeX code for a single Hebrew Bible page.

    This function assembles a complete LaTeX document using the
    supplied verses.  It sets up a right‑to‑left environment, prints
    the header (book name, page number and reference range), lays out
    the Hebrew text with superscript footnote markers, and appends a
    footnote apparatus.  All footnotes are numbered sequentially
    starting from ``start_footnote``.

    Parameters
    ----------
    book_name : str
        The Hebrew name of the book (e.g. "משלי").
    page_number : int
        The running page number to display in the header.
    verses : list of :class:`Verse`
        Verses to include on this page.  They will be rendered in
        order.
    lexicon : :class:`HebrewStrongLexicon`
        Lexicon used to produce definitions for footnotes.
    start_footnote : int, optional
        Starting footnote number.  Defaults to 1.

    hebrew_font : str, optional
        Name of the OpenType font to use for Hebrew text.  A font
        providing comprehensive coverage of Hebrew characters (for
        example ``SBL BibLit`` or ``SBL Hebrew``) is recommended so
        that cantillation marks and unusual diacritics such as the
        meteg (U+05BD) are available.  Defaults to ``'SBL BibLit'``.

    Returns
    -------
    str
        A string containing the LaTeX code for the page.  The code is
        a complete document (it includes ``\documentclass`` and
        ``\begin{document}``), so it can be compiled as‑is with
        XeLaTeX or LuaLaTeX.
    """
    # Assign footnotes to each word
    assign_footnotes(verses, lexicon, start=start_footnote)
    # Compute the reference string
    reference = compute_reference(verses)
    # Build the LaTeX document
    lines: List[str] = []
    lines.append(r'\documentclass[12pt]{article}')
    # fontspec allows selection of TrueType/OpenType fonts with XeLaTeX or LuaLaTeX
    lines.append(r'\usepackage{fontspec}')
    # expl3 is required by bidi/polyglossia on some TeX installations
    lines.append(r'\usepackage{expl3}')
    # bidi enables right‑to‑left typesetting; it depends on expl3
    lines.append(r'\usepackage{bidi}')
    # We do not use polyglossia here because older TeX distributions may
    # not support its internal LaTeX3 macros.  Instead we define a Hebrew
    # font explicitly.  If you have polyglossia available you can add
    # \setdefaultlanguage{english} and \setotherlanguage{hebrew} yourself.
    # Use a common Hebrew font if available; adjust as necessary.
    lines.append(rf'\newfontfamily\hebrewfont[Script=Hebrew]{{{hebrew_font}}}')
    lines.append('')
    lines.append(r'\begin{document}')
    # Header
    lines.append(r'\thispagestyle{plain}')
    lines.append(r'\begin{flushright}')
    # Book name and page number; the book name uses the Hebrew font
    lines.append(rf'{{\hebrewfont\Large {book_name}}} \hfill {{\small {page_number}}}\\')
    lines.append(rf'{{\small {reference}}}')
    lines.append(r'\end{flushright}')
    lines.append('')
    # Main text: iterate over verses
    lines.append(r'\begin{RTL}')  # right‑to‑left environment
    for verse in verses:
        # Print verse number at the right margin in boldface
        vnum = verse.verse_number
        # Start a new line for each verse
        lines.append(r'\noindent')
        # Print verse number in a box similar to the printed edition
        # Output the verse number in bold.  We avoid the \Arabic macro here
        # because it expects a counter rather than a literal number.
        lines.append(rf'\textbf{{{vnum}}} ')
        # Print words with footnote markers; separate by spaces
        word_parts: List[str] = []
        for word in verse.words:
            # Escape special LaTeX characters in the word text
            escaped_text = escape_latex(word.text)
            marker = ''
            if word.footnote_number is not None:
                marker = rf'\textsuperscript{{{word.footnote_number}}}'
            # Wrap Hebrew text in \hebrewfont to ensure proper glyphs
            word_parts.append(rf'{{\hebrewfont {escaped_text}}}{marker}')
        # Join the words with spaces
        lines.append(' '.join(word_parts) + r'\par')
    lines.append(r'\end{RTL}')
    lines.append('')
    # Footnote apparatus
    lines.append(r'\vspace{1em}')
    lines.append(r'\begin{footnotesize}')
    for verse in verses:
        for word in verse.words:
            fn = word.footnote_number
            ft = word.footnote_text or ''
            # Escape special characters in footnote text
            ft_esc = escape_latex(ft)
            lines.append(rf'\textsuperscript{{{fn}}} {ft_esc}\\')
    lines.append(r'\end{footnotesize}')
    lines.append(r'\end{document}')
    return '\n'.join(lines)


def escape_latex(text: str) -> str:
    """Escape a string for safe inclusion in LaTeX.

    This helper converts characters that have special meaning in LaTeX
    (such as backslashes, braces, dollar signs, etc.) into escaped
    versions.  It does not attempt to handle Hebrew diacritics or
    right‑to‑left marks; those are assumed to be safe in a XeLaTeX
    context with `fontspec` and `bidi` loaded.

    Parameters
    ----------
    text : str
        Input string.

    Returns
    -------
    str
        Escaped string.
    """
    replacements = {
        '\\': r'\textbackslash{}',
        '&': r'\&',
        '%': r'\%',
        '$': r'\$',
        '#': r'\#',
        '_': r'\_',
        '{': r'\{',
        '}': r'\}',
        '^': r'\^{}',
        '~': r'\~{}',
    }
    result = []
    for char in text:
        if char in replacements:
            result.append(replacements[char])
        else:
            result.append(char)
    return ''.join(result)
