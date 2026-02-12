#!/usr/bin/env python3
"""Generate a standalone HTML page showing side-by-side diffs between two text files."""

import argparse
import difflib
import html
import os
import sys
from pathlib import Path


def read_file(path: str) -> list[str]:
    with open(path, encoding="utf-8", errors="replace") as f:
        return f.readlines()


def build_html(left_path: str, right_path: str, colormode: str, context_lines: int = 3) -> str:
    left_lines = read_file(left_path)
    right_lines = read_file(right_path)

    left_name = os.path.basename(left_path)
    right_name = os.path.basename(right_path)

    matcher = difflib.SequenceMatcher(None, left_lines, right_lines)
    opcodes = matcher.get_opcodes()

    # Build rows: each row is (left_lineno|None, left_text|"", right_lineno|None, right_text|"", row_type)
    rows: list[tuple] = []

    for tag, i1, i2, j1, j2 in opcodes:
        if tag == "equal":
            # Context collapsing: show first/last `context_lines` and collapse middle
            n = i2 - i1
            if n > 2 * context_lines + 1:
                for k in range(context_lines):
                    rows.append((i1 + k + 1, left_lines[i1 + k], j1 + k + 1, right_lines[j1 + k], "equal"))
                rows.append((None, "", None, "", "fold"))
                for k in range(context_lines):
                    li = i2 - context_lines + k
                    ri = j2 - context_lines + k
                    rows.append((li + 1, left_lines[li], ri + 1, right_lines[ri], "equal"))
            else:
                for k in range(n):
                    rows.append((i1 + k + 1, left_lines[i1 + k], j1 + k + 1, right_lines[j1 + k], "equal"))
        elif tag == "replace":
            max_len = max(i2 - i1, j2 - j1)
            for k in range(max_len):
                li = i1 + k if k < i2 - i1 else None
                ri = j1 + k if k < j2 - j1 else None
                lt = left_lines[li] if li is not None else ""
                rt = right_lines[ri] if ri is not None else ""
                ln = (li + 1) if li is not None else None
                rn = (ri + 1) if ri is not None else None
                rows.append((ln, lt, rn, rt, "replace"))
        elif tag == "delete":
            for k in range(i1, i2):
                rows.append((k + 1, left_lines[k], None, "", "delete"))
        elif tag == "insert":
            for k in range(j1, j2):
                rows.append((None, "", k + 1, right_lines[k], "insert"))

    # Inline word-level highlighting for replace rows
    def word_diff_highlight(old: str, new: str, cls_del: str, cls_ins: str) -> tuple[str, str]:
        sm = difflib.SequenceMatcher(None, old, new)
        left_parts, right_parts = [], []
        for op, ai1, ai2, bi1, bi2 in sm.get_opcodes():
            a_seg = html.escape(old[ai1:ai2])
            b_seg = html.escape(new[bi1:bi2])
            if op == "equal":
                left_parts.append(a_seg)
                right_parts.append(b_seg)
            elif op == "replace":
                left_parts.append(f'<span class="{cls_del}">{a_seg}</span>')
                right_parts.append(f'<span class="{cls_ins}">{b_seg}</span>')
            elif op == "delete":
                left_parts.append(f'<span class="{cls_del}">{a_seg}</span>')
            elif op == "insert":
                right_parts.append(f'<span class="{cls_ins}">{b_seg}</span>')
        return "".join(left_parts), "".join(right_parts)

    # Build table rows HTML
    tbody_parts = []
    for left_no, left_text, right_no, right_text, row_type in rows:
        lt = left_text.rstrip("\n\r")
        rt = right_text.rstrip("\n\r")

        if row_type == "fold":
            tbody_parts.append(
                '<tr class="fold"><td class="ln"></td><td class="code" colspan="3">'
                '<svg width="12" height="12" viewBox="0 0 16 16"><path fill="currentColor" '
                'd="M0 5l6-3 6 3-6 3zm0 4l6 3 6-3" fill-rule="evenodd"/></svg> …</td></tr>'
            )
            continue

        left_cls = right_cls = ""
        left_html = html.escape(lt)
        right_html = html.escape(rt)

        if row_type == "replace":
            left_cls = "del"
            right_cls = "ins"
            left_html, right_html = word_diff_highlight(lt, rt, "wd", "wi")
        elif row_type == "delete":
            left_cls = "del"
        elif row_type == "insert":
            right_cls = "ins"

        ln_l = str(left_no) if left_no else ""
        ln_r = str(right_no) if right_no else ""

        tbody_parts.append(
            f'<tr>'
            f'<td class="ln {left_cls}">{ln_l}</td>'
            f'<td class="code {left_cls}"><pre>{left_html}</pre></td>'
            f'<td class="ln {right_cls}">{ln_r}</td>'
            f'<td class="code {right_cls}"><pre>{right_html}</pre></td>'
            f'</tr>'
        )

    tbody_html = "\n".join(tbody_parts)

    # Stats
    additions = sum(1 for *_, t in rows if t in ("insert",))
    deletions = sum(1 for *_, t in rows if t in ("delete",))
    modifications = sum(1 for *_, t in rows if t == "replace")

    dark = colormode.lower() == "dark"

    page_html = f"""<!DOCTYPE html>
<html lang="en" data-theme="{"dark" if dark else "light"}">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Diff: {html.escape(left_name)} ↔ {html.escape(right_name)}</title>
<style>
:root[data-theme="light"] {{
  --bg: #ffffff;
  --bg2: #f6f8fa;
  --fg: #1f2328;
  --fg2: #656d76;
  --border: #d1d9e0;
  --del-bg: #ffebe9;
  --del-line-bg: #fff5f5;
  --del-word: #ff8182;
  --ins-bg: #dafbe1;
  --ins-line-bg: #f0fff4;
  --ins-word: #7ee787;
  --fold-bg: #ddf4ff;
  --fold-fg: #0969da;
  --ln-fg: #8b949e;
  --header-bg: #f6f8fa;
  --badge-del: #cf222e;
  --badge-ins: #1a7f37;
  --badge-mod: #9a6700;
  --scrollbar-thumb: #c1c1c1;
}}
:root[data-theme="dark"] {{
  --bg: #0d1117;
  --bg2: #161b22;
  --fg: #e6edf3;
  --fg2: #8b949e;
  --border: #30363d;
  --del-bg: rgba(248,81,73,0.15);
  --del-line-bg: rgba(248,81,73,0.10);
  --del-word: rgba(248,81,73,0.4);
  --ins-bg: rgba(63,185,80,0.15);
  --ins-line-bg: rgba(63,185,80,0.10);
  --ins-word: rgba(63,185,80,0.4);
  --fold-bg: rgba(56,139,253,0.10);
  --fold-fg: #58a6ff;
  --ln-fg: #6e7681;
  --header-bg: #161b22;
  --badge-del: #f85149;
  --badge-ins: #3fb950;
  --badge-mod: #d29922;
  --scrollbar-thumb: #484f58;
}}
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
  background: var(--bg);
  color: var(--fg);
  line-height: 1.5;
}}
.container {{ max-width: 1440px; margin: 0 auto; padding: 16px; }}
.header {{
  display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap;
  gap: 12px; padding: 12px 16px; margin-bottom: 8px;
  background: var(--header-bg); border: 1px solid var(--border); border-radius: 8px;
}}
.header h1 {{ font-size: 16px; font-weight: 600; }}
.stats {{ display: flex; gap: 8px; font-size: 13px; font-weight: 600; }}
.stats .badge {{ padding: 2px 8px; border-radius: 12px; color: #fff; }}
.stats .badge.del {{ background: var(--badge-del); }}
.stats .badge.ins {{ background: var(--badge-ins); }}
.stats .badge.mod {{ background: var(--badge-mod); }}
.file-labels {{
  display: grid; grid-template-columns: 1fr 1fr; gap: 1px;
  background: var(--border); border: 1px solid var(--border); border-radius: 8px 8px 0 0;
  overflow: hidden;
}}
.file-labels .label {{
  padding: 8px 16px; font-size: 13px; font-weight: 600;
  background: var(--header-bg); color: var(--fg2);
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}}
.diff-table-wrapper {{
  border: 1px solid var(--border); border-top: none; border-radius: 0 0 8px 8px;
  overflow-x: auto;
}}
.diff-table-wrapper::-webkit-scrollbar {{ height: 8px; }}
.diff-table-wrapper::-webkit-scrollbar-thumb {{ background: var(--scrollbar-thumb); border-radius: 4px; }}
table.diff {{
  width: 100%; border-collapse: collapse; table-layout: fixed;
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, "Liberation Mono", monospace;
  font-size: 12px;
}}
table.diff td {{ padding: 0; vertical-align: top; }}
table.diff td.ln {{
  width: 50px; min-width: 50px; text-align: right; padding: 0 8px;
  color: var(--ln-fg); user-select: none; background: var(--bg2);
  border-right: 1px solid var(--border);
}}
table.diff td.code {{
  padding: 0 12px; white-space: pre; overflow: hidden;
}}
table.diff td.code pre {{
  margin: 0; font: inherit; white-space: pre;
  line-height: 20px;
}}
table.diff td.ln:nth-child(3) {{ border-left: 1px solid var(--border); }}
/* Row colors */
table.diff td.del {{ background: var(--del-bg); }}
table.diff td.ln.del {{ background: var(--del-line-bg); }}
table.diff td.ins {{ background: var(--ins-bg); }}
table.diff td.ln.ins {{ background: var(--ins-line-bg); }}
/* Inline word highlights */
span.wd {{ background: var(--del-word); border-radius: 3px; padding: 1px 0; }}
span.wi {{ background: var(--ins-word); border-radius: 3px; padding: 1px 0; }}
/* Fold row */
tr.fold td {{ background: var(--fold-bg); color: var(--fold-fg); padding: 4px 12px;
  font-size: 12px; cursor: default; text-align: center; }}
tr.fold td svg {{ vertical-align: -1px; margin-right: 4px; }}
/* Theme toggle */
.theme-toggle {{
  background: none; border: 1px solid var(--border); border-radius: 6px;
  color: var(--fg); cursor: pointer; padding: 4px 10px; font-size: 13px;
  display: flex; align-items: center; gap: 6px;
}}
.theme-toggle:hover {{ background: var(--bg2); }}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1>Diff</h1>
    <div class="stats">
      <span class="badge del">−{deletions}</span>
      <span class="badge ins">+{additions}</span>
      <span class="badge mod">~{modifications}</span>
    </div>
    <button class="theme-toggle" onclick="toggleTheme()" title="Toggle light/dark mode">
      <svg id="icon-sun" width="14" height="14" viewBox="0 0 16 16" style="display:{"none" if dark else "inline"}">
        <path fill="currentColor" d="M8 12a4 4 0 1 0 0-8 4 4 0 0 0 0 8zm0-1.5a2.5 2.5 0 1 1 0-5 2.5 2.5 0 0 1 0 5zM8 0a.75.75 0 0 1 .75.75v1.5a.75.75 0 0 1-1.5 0V.75A.75.75 0 0 1 8 0zm0 13a.75.75 0 0 1 .75.75v1.5a.75.75 0 0 1-1.5 0v-1.5A.75.75 0 0 1 8 13zM16 8a.75.75 0 0 1-.75.75h-1.5a.75.75 0 0 1 0-1.5h1.5A.75.75 0 0 1 16 8zM3 8a.75.75 0 0 1-.75.75H.75a.75.75 0 0 1 0-1.5h1.5A.75.75 0 0 1 3 8z"/>
      </svg>
      <svg id="icon-moon" width="14" height="14" viewBox="0 0 16 16" style="display:{"inline" if dark else "none"}">
        <path fill="currentColor" d="M9.598 1.591a.749.749 0 0 1 .785-.175 7.001 7.001 0 1 1-8.967 8.967.75.75 0 0 1 .961-.96A5.5 5.5 0 0 0 9.77 2.417a.75.75 0 0 1-.172-.826z"/>
      </svg>
      <span id="theme-label">{"Dark" if dark else "Light"}</span>
    </button>
  </div>
  <div class="file-labels">
    <div class="label" title="{html.escape(left_path)}">{html.escape(left_name)}</div>
    <div class="label" title="{html.escape(right_path)}">{html.escape(right_name)}</div>
  </div>
  <div class="diff-table-wrapper">
    <table class="diff">
      <tbody>
{tbody_html}
      </tbody>
    </table>
  </div>
</div>
<script>
function toggleTheme() {{
  const root = document.documentElement;
  const current = root.getAttribute("data-theme");
  const next = current === "dark" ? "light" : "dark";
  root.setAttribute("data-theme", next);
  document.getElementById("icon-sun").style.display = next === "light" ? "inline" : "none";
  document.getElementById("icon-moon").style.display = next === "dark" ? "inline" : "none";
  document.getElementById("theme-label").textContent = next === "dark" ? "Dark" : "Light";
}}
</script>
</body>
</html>"""
    return page_html


def main():
    parser = argparse.ArgumentParser(
        description="Generate a standalone HTML diff page comparing two text files."
    )
    parser.add_argument("left", help="Path to the left (original) file")
    parser.add_argument("right", help="Path to the right (modified) file")
    parser.add_argument(
        "--colormode", "-c",
        choices=["light", "dark"],
        default="light",
        help="Color theme: light or dark (default: light)",
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Output HTML file path (default: diff.html in current directory)",
    )
    parser.add_argument(
        "--context", "-n",
        type=int,
        default=3,
        help="Number of context lines around changes (default: 3)",
    )
    args = parser.parse_args()

    for p in (args.left, args.right):
        if not os.path.isfile(p):
            print(f"Error: file not found: {p}", file=sys.stderr)
            sys.exit(1)

    left_stem = Path(args.left).stem
    right_stem = Path(args.right).stem
    output = args.output or f"diff___{left_stem}___{right_stem}.html"
    page = build_html(args.left, args.right, args.colormode, args.context)
    Path(output).write_text(page, encoding="utf-8")
    print(f"Diff written to {output}")


if __name__ == "__main__":
    main()
