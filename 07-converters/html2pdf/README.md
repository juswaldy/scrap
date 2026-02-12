# html2pdf

Convert an HTML file into a **single long one-page PDF** with a fixed width.

## Setup

```bash
cd html2pdf
npm install
npx playwright install chromium
```

## Convert

From the repo root (or anywhere), run:

```bash
node html2pdf/convert.js "Datahub Maintenance Processes.html" "Datahub Maintenance Processes.onepage.8.5in.pdf" 8.5
```

The 3rd argument is the PDF width in inches (defaults to `8.5`).
