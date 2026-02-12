# diffpage

Generate a standalone HTML page showing side-by-side diffs between two text files, styled like GitHub / Diffchecker.

## Features

- Side-by-side diff view with line numbers
- Inline word-level change highlighting
- Light and dark color themes (with toggle button in the output)
- Context collapsing for unchanged regions
- Zero dependencies — stdlib only

## Usage

```bash
python diffpage.py <left_file> <right_file> --colormode light|dark [--output mydiff.html] [--context 3]
```

### Parameters

| Param | Required | Description |
|---|---|---|
| `left` | Yes | Path to the original file |
| `right` | Yes | Path to the modified file |
| `--colormode`, `-c` | No | `light` or `dark` (default: `light`) |
| `--output`, `-o` | No | Output HTML path (default: `diff___<left_file_stem>___<right_file_stem>.html`) |
| `--context`, `-n` | No | Context lines around changes (default: `3`) |

### Examples

```bash
python diffpage.py old.txt new.txt -c dark -o diff___old___new.html
python diffpage.py file_a.py file_b.py --colormode light --context 5
```

Open the generated HTML file in any browser — no server required.
