import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { chromium } from 'playwright';
import PDFDocument from 'pdfkit';

function usageAndExit() {
  console.error('Usage: node convert.js <input.html> <output.pdf> [width_in=8.5]');
  process.exit(2);
}

const inputArg = process.argv[2];
const outputArg = process.argv[3];
const widthIn = Number(process.argv[4] ?? '8.5');

if (!inputArg || !outputArg || !Number.isFinite(widthIn) || widthIn <= 0) usageAndExit();

const inputPath = path.resolve(process.cwd(), inputArg);
const outputPath = path.resolve(process.cwd(), outputArg);
const tmpPngPath = outputPath.replace(/\.pdf$/i, '') + '.tmp.png';

if (!fs.existsSync(inputPath)) {
  console.error(`Input not found: ${inputPath}`);
  process.exit(1);
}

const CSS_PX_PER_IN = 96;
const widthPx = Math.round(widthIn * CSS_PX_PER_IN);
const widthPt = widthIn * 72;

const fileUrl = pathToFileURL(inputPath).toString();

const browser = await chromium.launch();
const context = await browser.newContext({
  viewport: { width: widthPx, height: 800 },
  deviceScaleFactor: 1
});
const page = await context.newPage();

await page.goto(fileUrl, { waitUntil: 'load' });
await page.evaluate(async () => {
  // Best-effort wait for web fonts.
  if (document.fonts?.ready) await document.fonts.ready;
});

const scrollHeight = await page.evaluate(() => Math.max(
  document.body?.scrollHeight ?? 0,
  document.documentElement?.scrollHeight ?? 0,
  document.body?.offsetHeight ?? 0,
  document.documentElement?.offsetHeight ?? 0
));

await page.screenshot({ path: tmpPngPath, fullPage: true });
await browser.close();

// Convert PNG -> a single-page PDF with fixed width and computed height.
const heightPt = widthPt * (scrollHeight / widthPx);

await new Promise((resolve, reject) => {
  const doc = new PDFDocument({
    size: [widthPt, heightPt],
    margin: 0
  });

  const out = fs.createWriteStream(outputPath);
  out.on('error', reject);
  doc.on('error', reject);
  out.on('finish', resolve);

  doc.pipe(out);
  doc.image(tmpPngPath, 0, 0, { width: widthPt });
  doc.end();
});

fs.rmSync(tmpPngPath, { force: true });
console.log(`Wrote: ${outputPath}`);
