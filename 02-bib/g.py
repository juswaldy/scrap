from bible_page_generator import HebrewBibleParser, HebrewStrongLexicon, generate_latex_page

# Load the book of Proverbs from the WLC
parser = HebrewBibleParser()
chapters = parser.load_book('https://github.com/openscriptures/morphhb/blob/master/wlc/Prov.xml')

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
lexicon.load('https://github.com/openscriptures/HebrewLexicon/blob/master/HebrewStrong.xml')

# Render LaTeX
latex = generate_latex_page(
    book_name='משלי',
    page_number=1329,
    verses=verses,
    lexicon=lexicon,
    start_footnote=90  # match the numbering in the scanned page
)
# Write LaTeX to file
with open('./Proverbs_1329.tex', 'w', encoding='utf-8') as f:
    f.write(latex)
