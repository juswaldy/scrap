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
