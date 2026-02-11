#!/usr/bin/env python3
"""
Markdown to HTML batch converter with TOC and client-side Mermaid rendering.

Usage:
    python md2html.py --input_folder /path/to/mds --output_folder /path/to/out
    # If arguments are omitted, you'll be prompted interactively.

Requirements:
    - Python 3.8+
    - The 'markdown' package: pip install markdown
"""
import argparse
import sys
import re
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional, Dict

# -------- Utilities --------

MERMAID_CDN = "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"

BASE_CSS = """
:root {
  --fg: #1f2937;
  --bg: #ffffff;
  --muted: #6b7280;
  --link: #2563eb;
  --border: #e5e7eb;
  --code-bg: #f8fafc;
  --kbd-bg: #f3f4f6;
  --maxw: 900px;
}
* { box-sizing: border-box; }
html, body { margin: 0; padding: 0; background: var(--bg); color: var(--fg); }
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Inter, "Helvetica Neue", Arial, "Noto Sans", "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol"; line-height: 1.6; }
.container { max-width: var(--maxw); margin: 2.5rem auto; padding: 0 1rem; }
nav.page-nav { display: flex; justify-content: space-between; gap: 0.75rem; margin: 1rem 0 2rem; flex-wrap: wrap; }
nav.page-nav a { text-decoration: none; color: var(--link); border: 1px solid var(--border); padding: .45rem .7rem; border-radius: 10px; }
h1, h2, h3 { line-height: 1.25; }
h1 { font-size: 2rem; margin-top: 0; }
h2 { font-size: 1.5rem; }
article { border-top: 1px solid var(--border); padding-top: 1rem; }
pre, code { background: var(--code-bg); }
pre { padding: 1rem; border-radius: 10px; overflow-x: auto; border: 1px solid var(--border); }
code { border-radius: 6px; } /*jjjpadding: 0 .25rem; }*/
blockquote { border-left: 4px solid var(--border); margin: 1rem 0; padding: .25rem 1rem; color: var(--muted); }
table { border-collapse: collapse; width: 100%; margin: 1rem 0; }
th, td { border: 1px solid var(--border); padding: .5rem .6rem; text-align: left; }
hr { border: none; border-top: 1px solid var(--border); margin: 2rem 0; }
.toc-list { list-style: none; padding: 0; margin: 1rem 0 0; }
.toc-list li { margin: 0.5rem 0; }
.toc-date { color: var(--muted); font-size: .9rem; margin-left: .35rem; }
.badge { display: inline-block; font-size: .8rem; color: #065f46; background: #ecfdf5; border: 1px solid #a7f3d0; padding: .2rem .45rem; border-radius: 999px; }
.alert { padding: .75rem 1rem; border-radius: 10px; border: 1px solid #fecaca; background: #fff1f2; color: #7f1d1d; }
footer { margin-top: 2rem; font-size: .9rem; color: var(--muted); }
"""

HTML_SHELL = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>{css}</style>
<link rel="icon" href="data:,">
</head>
<body>
<div class="container">
<header>
  <h1>{title}</h1>
  <p><span class="badge">Generated</span> {site_note}</p>
</header>
{nav_top}
<article>
{content}
</article>
{nav_bottom}
<footer>
  <p>Built from Markdown. Mermaid rendered client-side.</p>
</footer>
</div>

<script defer src="{mermaid_cdn}"></script>
<script>
// Mermaid handling: render existing <div class="mermaid"> blocks and convert residual code fences if present.
// If rendering fails, replace block with a friendly error message.
(function() {{
  function collectMermaidDivs() {{
    return Array.from(document.querySelectorAll('div.mermaid'));
  }}
  function convertResidualCodeFences() {{
    // Handle cases where Markdown left <pre><code class="language-mermaid">…</code></pre>
    const codeNodes = Array.from(document.querySelectorAll('pre code.language-mermaid, pre code.mermaid, code.language-mermaid'));
    codeNodes.forEach(codeEl => {{
      const raw = codeEl.textContent;
      const pre = codeEl.closest('pre') || codeEl.parentElement;
      const div = document.createElement('div');
      div.className = 'mermaid';
      div.textContent = raw;
      (pre || codeEl).replaceWith(div);
    }});
  }}
  function showMermaidError(el, err) {{
    const msg = document.createElement('div');
    msg.className = 'alert';
    msg.innerHTML = '<strong>Mermaid diagram failed to render.</strong><br>' +
                    (err && err.message ? err.message : 'Unknown error.');
    el.replaceWith(msg);
  }}
  function initMermaidRender() {{
    if (!window.mermaid) return;
    try {{ window.mermaid.initialize({{ startOnLoad: false }}); }} catch (e) {{ /* ignore */ }}
    const blocks = collectMermaidDivs();
    blocks.forEach((el, i) => {{
      const src = el.textContent;
      const id = 'mmd-' + i;
      try {{
        const out = window.mermaid.render(id, src);
        if (out && typeof out.then === 'function') {{
          out.then(res => {{ el.innerHTML = res.svg; }}).catch(err => showMermaidError(el, err));
        }} else if (out && out.svg) {{
          el.innerHTML = out.svg;
        }} else {{
          showMermaidError(el, new Error('Unknown Mermaid render result.'));
        }}
      }} catch (err) {{
        showMermaidError(el, err);
      }}
    }});
  }}
  document.addEventListener('DOMContentLoaded', function() {{
    convertResidualCodeFences();
    if (window.mermaid) {{ initMermaidRender(); }}
    else {{ window.addEventListener('load', initMermaidRender); }}
  }});
}})();
</script>
</body>
</html>
"""

TOC_SHELL = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Table of Contents</title>
<style>{css}</style>
<link rel="icon" href="data:,">
</head>
<body>
<div class="container">
<header>
  <h1>Table of Contents</h1>
  <p>Generated from Markdown files in lexicographic order.</p>
</header>
<ol class="toc-list">
{items}
</ol>
<footer>
  <p>{count} documents.</p>
</footer>
</div>
</body>
</html>
"""

FRONTMATTER_RE = re.compile(r'^---\s*\n(.*?)\n---\s*\n', re.DOTALL)
TITLE_KV_RE = re.compile(r'^\s*title\s*:\s*(.+)\s*$', re.MULTILINE | re.IGNORECASE)
DATE_KV_RE  = re.compile(r'^\s*date\s*:\s*(.+)\s*$', re.MULTILINE | re.IGNORECASE)

# Fenced mermaid blocks: ```mermaid ... ``` or ~~~mermaid ... ~~~
MERMAID_FENCE_RE = re.compile(
    r'(?P<fence>```|~~~)\s*mermaid\s*\n(?P<body>.*?)(?:\n)?(?P=fence)\s*',
    re.DOTALL | re.IGNORECASE
)

def extract_frontmatter(md_text: str) -> Tuple[Optional[Dict[str, str]], str]:
    m = FRONTMATTER_RE.match(md_text)
    if not m:
        return None, md_text
    fm_text = m.group(1)
    fm: Dict[str, str] = {}
    t = TITLE_KV_RE.search(fm_text)
    if t:
        fm['title'] = t.group(1).strip().strip('"').strip("'")
    d = DATE_KV_RE.search(fm_text)
    if d:
        fm['date'] = d.group(1).strip().strip('"').strip("'")
    rest = md_text[m.end():]
    return (fm if fm else None), rest

def extract_title(md_text: str, fallback: str) -> str:
    fm, body = extract_frontmatter(md_text)
    if fm and 'title' in fm:
        return fm['title']
    h1 = re.search(r'^\s*#\s+(.+?)\s*$', body, re.MULTILINE)
    if h1:
        return h1.group(1).strip()
    return fallback

def replace_mermaid_fences(md_text: str) -> str:
    def _sub(m: re.Match) -> str:
        code = m.group('body')
        return '<div class="mermaid">\n' + html_escape(code) + '\n</div>'
    return MERMAID_FENCE_RE.sub(_sub, md_text)

def find_markdown_files(input_dir: Path) -> List[Path]:
    return sorted([p for p in input_dir.iterdir() if p.is_file() and p.suffix.lower() == '.md'],
                  key=lambda p: p.name.lower())

def read_text(path: Path) -> Optional[str]:
    try:
        return path.read_text(encoding='utf-8')
    except Exception as e:
        print(f"[WARN] Cannot read {path.name}: {e}")
        return None

def ensure_output_dir(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        return True
    except Exception as e:
        print(f"[ERROR] Cannot create output directory: {e}")
        return False

def convert_markdown(md_text: str) -> str:
    """Convert Markdown to HTML. Requires the 'markdown' package'."""
    try:
        import markdown  # type: ignore
    except ImportError:
        print("[ERROR] Missing dependency: 'markdown'. Install with: pip install markdown")
        raise
    exts = ['extra', 'codehilite', 'toc', 'sane_lists', 'smarty', 'fenced_code']
    html = markdown.markdown(md_text, extensions=exts)
    return html

def build_nav(prev_href: Optional[str], up_href: str, next_href: Optional[str]) -> str:
    def link(href: Optional[str], label: str) -> str:
        return f'<a href="{href}">{label}</a>' if href else f'<span style="opacity:.5">{label}</span>'
    return f"""<nav class="page-nav">
        {link(prev_href, "← Previous")}
        {link(up_href, "↑ TOC")}
        {link(next_href, "Next →")}
    </nav>"""

def write_file(path: Path, text: str) -> bool:
    try:
        path.write_text(text, encoding='utf-8')
        return True
    except Exception as e:
        print(f"[ERROR] Failed to write {path.name}: {e}")
        return False

def make_filename(base: str) -> str:
    import re as _re
    return _re.sub(r'[^a-zA-Z0-9._-]+', '-', base).strip('-') + '.html'

def parse_date_for_display(frontmatter_date: Optional[str], src_path: Path) -> Tuple[str, str]:
    """Returns (display_str, iso_attr). If no frontmatter date, use file mtime."""
    if frontmatter_date:
        # Try simple ISO formats first
        for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(frontmatter_date, fmt)
                return (dt.strftime("%Y-%m-%d"), dt.isoformat())
            except ValueError:
                pass
        # Fallback: return as-is for display, no strict ISO
        return (frontmatter_date, frontmatter_date)
    # Fallback to file modified time
    try:
        ts = src_path.stat().st_mtime
        dt = datetime.fromtimestamp(ts)
        return (dt.strftime("%Y-%m-%d"), dt.isoformat())
    except Exception:
        return ("", "")

def main():
    parser = argparse.ArgumentParser(description="Convert Markdown files to HTML with TOC and Mermaid.")
    parser.add_argument('--input_folder', type=str, help='Path to folder containing Markdown files')
    parser.add_argument('--output_folder', type=str, help='Path to folder for HTML output and toc.html')
    args = parser.parse_args()

    input_folder = args.input_folder or input("Enter input folder path (with .md files): ").strip()
    output_folder = args.output_folder or input("Enter output folder path (for HTML files): ").strip()

    in_path = Path(input_folder).expanduser().resolve()
    out_path = Path(output_folder).expanduser().resolve()

    # Validate paths
    if not in_path.exists() or not in_path.is_dir():
        print(f"[ERROR] Invalid input folder: {in_path}")
        sys.exit(2)
    if not ensure_output_dir(out_path):
        print("[ERROR] Output directory invalid or not creatable.")
        sys.exit(2)
    print(f"[OK] Paths validated. Input: {in_path} | Output: {out_path}")

    # Collect and sort Markdown files
    md_files = find_markdown_files(in_path)
    if not md_files:
        print("[ERROR] No .md files found in input folder.")
        sys.exit(1)
    print(f"[OK] Found {len(md_files)} markdown files. Proceeding to convert.")

    # Pre-scan titles, dates, and target filenames
    entries: List[Dict[str, object]] = []
    for p in md_files:
        raw = read_text(p)
        if raw is None:
            print(f"[WARN] Skipping unreadable file: {p.name}")
            continue
        fm, body = extract_frontmatter(raw)
        title = fm.get('title') if fm and 'title' in fm else None
        title = title or extract_title(raw, fallback=p.stem)
        date_display, date_iso = parse_date_for_display((fm.get('date') if fm else None), p)
        out_name = make_filename(p.stem)
        entries.append({"src": p, "title": title, "date_display": date_display, "date_iso": date_iso, "out_name": out_name, "body": body})

    if not entries:
        print("[ERROR] No readable markdown files after scanning.")
        sys.exit(1)
    print(f"[OK] Titles/dates extracted. Preparing HTML generation for {len(entries)} files.")

    # Generate files
    success_count = 0
    for idx, ent in enumerate(entries):
        src: Path = ent["src"]  # type: ignore
        title: str = ent["title"]  # type: ignore
        out_name: str = ent["out_name"]  # type: ignore
        body: str = ent["body"]  # type: ignore

        prev_href = entries[idx-1]["out_name"] if idx > 0 else None
        next_href = entries[idx+1]["out_name"] if idx < len(entries) - 1 else None
        up_href = "toc.html"

        # Preprocess Mermaid fences and convert
        preprocessed = replace_mermaid_fences(body)
        try:
            html_body = convert_markdown(preprocessed)
        except Exception as e:
            print(f"[WARN] Skipping {src.name}: conversion failed ({e}).")
            continue

        # Build page with navigation
        nav_top = build_nav(prev_href, up_href, next_href)
        nav_bottom = build_nav(prev_href, up_href, next_href)
        site_note = f"from {src.name}"
        page_html = HTML_SHELL.format(
            title=html_escape(str(title)),
            css=BASE_CSS,
            nav_top=nav_top,
            nav_bottom=nav_bottom,
            content=html_body,
            site_note=html_escape(site_note),
            mermaid_cdn=MERMAID_CDN
        )

        out_file = out_path / out_name
        if write_file(out_file, page_html):
            success_count += 1
            print(f"[OK] Wrote {out_file.name}")
        else:
            print(f"[ERROR] Failed to write {out_file.name}")

    if success_count == 0:
        print("[ERROR] No documents were generated. Aborting before TOC.")
        sys.exit(1)
    else:
        print(f"[OK] Generated {success_count} HTML documents. Building TOC.")

    # Build TOC (only include successfully written files)
    items_html = []
    included = 0
    for ent in entries:
        out_name = ent["out_name"]  # type: ignore
        if (out_path / out_name).exists():
            title = html_escape(str(ent["title"]))  # type: ignore
            date_disp = str(ent["date_display"])  # type: ignore
            date_iso  = str(ent["date_iso"])      # type: ignore
            date_bit = f' <time class="toc-date" datetime="{html_escape(date_iso)}">{html_escape(date_disp)}</time>' if date_disp else ""
            items_html.append(f'<li><a href="{out_name}">{title}</a>{date_bit}</li>')
            included += 1

    toc_html = TOC_SHELL.format(css=BASE_CSS, items="\n".join(items_html), count=included)
    if write_file(out_path / "toc.html", toc_html):
        print("[OK] toc.html generated with dates. Validate by opening it in a browser.")
    else:
        print("[ERROR] Failed to write toc.html")

    print("[DONE] Process complete.")

def html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
    )

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[INFO] Interrupted by user.")
        sys.exit(130)
