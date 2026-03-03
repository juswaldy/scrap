"""
Fix moonphases.csv so events are in chronological order left-to-right, top-to-bottom.
Groups events by year, then builds rows where each row has at most one NM, FQ, FM, LQ
in chronological order (NM < FQ < FM < LQ within a row).
"""
import csv
import re
from datetime import datetime
from io import StringIO
from collections import defaultdict

filepath = r'c:\github\scrap\12-astro\moonphases.csv'

# Read the file
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

lines = content.split('\n')
# Remove trailing empty line if present
if lines and lines[-1] == '':
    lines = lines[:-1]

header1 = lines[0]
header2 = lines[1]
data_lines = lines[2:]

# Phase order: NM=0, FQ=1, FM=2, LQ=3
PHASE_ORDER = {'NM': 0, 'FQ': 1, 'FM': 2, 'LQ': 3}

events = []
for line in data_lines:
    if not line.strip():
        continue

    reader = csv.reader(StringIO(line))
    try:
        row = next(reader)
    except StopIteration:
        continue

    # Pad row to 14 elements
    while len(row) < 14:
        row.append('')

    # Column specs: (phase, date_col, utc_col, eclipse_col_or_None)
    specs = [
        ('NM', 0, 1, 2),
        ('FQ', 4, 5, None),
        ('FM', 8, 9, 10),
        ('LQ', 12, 13, None),
    ]

    for phase, dc, uc, ec in specs:
        date_str = row[dc].strip()
        utc_str = row[uc].strip()
        eclipse_str = row[ec].strip() if ec is not None else ''

        if date_str and utc_str:
            match = re.match(r'(\w+),\s*(\d{4})-(\d{2})-(\d{2})', date_str)
            if match:
                year = int(match.group(2))
                month = int(match.group(3))
                day = int(match.group(4))
                h, m = utc_str.split(':')
                dt = datetime(year, month, day, int(h), int(m))
                events.append({
                    'phase': phase,
                    'datetime': dt,
                    'year': year,
                    'date_str': date_str,
                    'utc_str': utc_str,
                    'eclipse': eclipse_str,
                })

# Sort all events chronologically
events.sort(key=lambda e: e['datetime'])

# Group by year
events_by_year = defaultdict(list)
for e in events:
    events_by_year[e['year']].append(e)

# Build rows per year using greedy algorithm
all_rows = []
for year in sorted(events_by_year.keys()):
    year_events = events_by_year[year]  # already sorted

    current_row = {}
    last_order = -1

    for event in year_events:
        order = PHASE_ORDER[event['phase']]
        if order > last_order:
            current_row[event['phase']] = event
            last_order = order
        else:
            # Emit current row and start new one
            all_rows.append(current_row)
            current_row = {event['phase']: event}
            last_order = order

    if current_row:
        all_rows.append(current_row)


# Format time: strip leading zero from hour
def fmt_time(utc_str):
    h, m = utc_str.split(':')
    return f"{int(h)}:{m}"


# Build output lines
output_lines = [header1, header2]

for row in all_rows:
    fields = []

    # NM (cols 0-3): date, utc, eclipse, empty
    if 'NM' in row:
        e = row['NM']
        fields.extend([e['date_str'], fmt_time(e['utc_str']), e['eclipse'], ''])
    else:
        fields.extend(['', '', '', ''])

    # FQ (cols 4-7): date, utc, empty, empty
    if 'FQ' in row:
        e = row['FQ']
        fields.extend([e['date_str'], fmt_time(e['utc_str']), '', ''])
    else:
        fields.extend(['', '', '', ''])

    # FM (cols 8-11): date, utc, eclipse, empty
    if 'FM' in row:
        e = row['FM']
        fields.extend([e['date_str'], fmt_time(e['utc_str']), e['eclipse'], ''])
    else:
        fields.extend(['', '', '', ''])

    # LQ (cols 12-13): date, utc
    if 'LQ' in row:
        e = row['LQ']
        fields.extend([e['date_str'], fmt_time(e['utc_str'])])
    else:
        fields.extend(['', ''])

    # Write with csv to handle quoting (dates contain commas)
    buf = StringIO()
    writer = csv.writer(buf, lineterminator='')
    writer.writerow(fields)
    output_lines.append(buf.getvalue())

# Write output
output_text = '\n'.join(output_lines) + '\n'
with open(filepath, 'w', encoding='utf-8', newline='') as f:
    f.write(output_text)

print(f"Total events: {len(events)}")
print(f"Total rows: {len(all_rows)}")
print(f"Years: {min(events_by_year.keys())}-{max(events_by_year.keys())}")
print(f"\nFirst 16 lines of output:")
for i, line in enumerate(output_lines[:18], start=1):
    print(f"L{i:>3}: {line}")
