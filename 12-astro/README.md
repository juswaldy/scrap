# astro.py — 21st-Century Astronomical Event Generator

Generates comprehensive CSV datasets of astronomical events for the years **2001–2100**, sourced from authoritative online catalogs and computed via JPL ephemerides.

## Output Files

Running the script produces four CSV files (and optionally a combined Excel workbook):

| File | Contents |
|---|---|
| `moonphases.csv` | New Moon, First Quarter, Full Moon, and Last Quarter dates/times (UTC) with solar and lunar eclipse codes |
| `perigees.csv` | Lunar perigee and apogee dates/times (UTC), distances (km), and yearly min/max flags |
| `elongation.csv` | Greatest elongations (Mercury, Venus) and oppositions (Mars, Jupiter, Saturn, Uranus, Neptune) with angles |
| `meteors.csv` | Major meteor shower peak dates/times (UTC) for 2001–2100 with ZHR (Zenithal Hourly Rate), computed via solar longitude |
| `astro.xlsx` | *(optional)* All of the above combined into a single Excel workbook with one sheet per dataset |

## Data Sources

| Source | URL | Used For |
|---|---|---|
| AstroPixels — Moon Phases | https://www.astropixels.com/ephemeris/moon/phases2001gmt.html | New/Full/Quarter moon dates & times |
| AstroPixels — Perigee/Apogee | https://www.astropixels.com/ephemeris/moon/moonperap2001.html | Lunar distance extremes |
| NASA GSFC — Solar Eclipses | https://eclipse.gsfc.nasa.gov/SEcat5/SE2001-2100.html | Solar eclipse type codes (T/A/P) |
| NASA GSFC — Lunar Eclipses | https://eclipse.gsfc.nasa.gov/LEcat5/LE2001-2100.html | Lunar eclipse type codes (t/p/n) |
| JPL DE440s Ephemeris | https://ssd.jpl.nasa.gov/ftp/eph/planets/bsp/de440s.bsp | Planetary positions for elongation/opposition computation |
| IMO (parameters) | — | Meteor shower solar longitudes & typical ZHR (peak times computed per-year via JPL ephemeris) |

On first run, all sources are downloaded and cached in the `downloads/` directory (~34 MB for the JPL ephemeris). Subsequent runs can use `--offline` to skip network access entirely.

## Requirements

```
Python 3.9+
numpy
skyfield
openpyxl    # only needed for --xlsx
```

Install dependencies:

```bash
pip install numpy skyfield openpyxl
```

## Usage

```bash
# First run — downloads all sources, generates CSVs
python astro.py

# Offline mode — uses cached downloads only (no network)
python astro.py --offline

# Also generate a combined Excel workbook
python astro.py --offline --xlsx

# Custom output and download directories
python astro.py --out-dir ./output --download-dir ./cache
```

### CLI Options

| Flag | Default | Description |
|---|---|---|
| `--out-dir DIR` | Script directory | Where CSV (and xlsx) files are written |
| `--download-dir DIR` | `./downloads` | Where downloaded HTML and ephemeris files are cached |
| `--offline` | Off | Use only cached files; fail if any are missing |
| `--xlsx` | Off | Combine all CSVs into a single `astro.xlsx` workbook |

### Progress Output

The script prints numbered progress steps with elapsed time so you can track long runs:

```
[1/9] Loading cached HTML sources ...  (elapsed 0.0s)
[2/9] Parsing NASA eclipse catalogs ...  (elapsed 0.0s)
  Solar eclipses: 224, Lunar eclipses: 228
[3/9] Parsing moon phases ...  (elapsed 0.0s)
  Years with phase data: 100
[4/9] Parsing perigee/apogee data ...  (elapsed 0.0s)
  Perigee/apogee rows: 1374
[5/9] Computing planetary elongations & oppositions ...  (elapsed 0.1s)
  [1/7] Computing oppositions for Mars ... done (47 events)
  [2/7] Computing oppositions for Jupiter ... done (91 events)
  ...
  [7/7] Computing elongations for Venus ... done (126 events)
  Planetary events: 1188
[6/9] Computing meteor shower peaks ...  (elapsed 37.8s)
  [1/10] Computing peaks for Quadrantids ... done (100 peaks)
  ...
  [10/10] Computing peaks for Geminids ... done (100 peaks)
  Meteor shower peaks: 1000
[7/9] Computing perigee/apogee min/max flags ...  (elapsed 38.0s)
[8/9] Writing CSV files ...  (elapsed 38.0s)
[9/9] Writing astro.xlsx ...  (elapsed 38.1s)

Done in 39.0s.
```

## CSV Schemas

### moonphases.csv

| Column | Description |
|---|---|
| Date | Weekday + ISO date, e.g. `Wed, 2001-01-24` |
| UTC | Time in UTC (`HH:MM`) |
| Eclipse | Solar eclipse code on New Moon: **T** (total), **A** (annular), **P** (partial). Lunar eclipse code on Full Moon: **t** (total), **p** (partial), **n** (penumbral). Blank if no eclipse. |

Arranged in four column groups: New Moon, First Quarter, Full Moon, Last Quarter.

### perigees.csv

| Column | Description |
|---|---|
| Date | Weekday + ISO date |
| UTC | Time in UTC (`HH:MM`) |
| Distance | Earth–Moon distance in km |
| MinMax | **m** = closest perigee (or nearest apogee) of the year; **M** = farthest apogee (or most distant perigee) of the year |

Arranged in two column groups: Perigee, Apogee.

### elongation.csv

| Column | Description |
|---|---|
| Planet | Mercury, Venus, Mars, Jupiter, Saturn, Uranus, or Neptune |
| Event | `Greatest Elongation` (inner planets) or `Opposition` (outer planets) |
| Direction | **E** (east) or **W** (west) for elongations; blank for oppositions |
| Date | Weekday + ISO date |
| UTC | Time in UTC (`HH:MM`) |
| AngleDeg | Sun–Earth–Planet angle in degrees |

### meteors.csv

| Column | Description |
|---|---|
| Shower | Meteor shower name (Quadrantids, Lyrids, EAquarids, DAquarids, Perseids, Draconids, Orionids, Taurids, Leonids, Geminids) |
| Date | Weekday + ISO date of computed peak |
| UTC | Peak time in UTC (`HH:MM`), computed from the Sun reaching the shower's characteristic ecliptic longitude |
| ZHR | Zenithal Hourly Rate (expected meteors/hour under ideal conditions) |

10 showers × 100 years = 1,000 rows.

## Project Structure

```
12-astro/
├── astro.py            # Main script
├── README.md           # This file
├── moonphases.csv      # Generated output
├── perigees.csv        # Generated output
├── elongation.csv      # Generated output
├── meteors.csv         # Generated output
├── astro.xlsx          # Generated output (with --xlsx)
└── downloads/          # Cached HTML sources + JPL ephemeris
```
