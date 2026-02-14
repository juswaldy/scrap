# Downloads Organizer (Option A: Purpose-first)

A small, well-documented Python app to implement the **5-step** organization approach described in [00-plan.md](00-plan.md), using **Option A (Purpose-first)**.

## What it does (5 steps)
This app runs in safe, incremental stages:

1. **Create folder structure** (Option A) under the target directory.
2. **(Optional) Sweep root into `00-Inbox/`** (moves most items into Inbox to reduce clutter).
3. **Sort `00-Inbox/` – Pass 1**: move obvious buckets (`Media`, `Software`, `Archives`, `Ops & Backups`).
4. **Sort `00-Inbox/` – Pass 2**: route remaining files to `Work` vs `Reading` using heuristics.
5. **Sort `00-Inbox/` – Pass 3**: route to domain subfolders (Finance/Student/Identity/Integrations/etc.), and place “project-like” directories under `02-Projects/`.

All steps support **dry-run** mode so you can preview changes.

## Safety features
- Dry run (`--dry-run`) prints planned moves without changing anything.
- Never touches the organizer app folder itself (`downloads_organizer/`) or `00-plan.md`.
- Skips hidden files and directories by default.
- Refuses to operate outside the provided `--root`.

## Quick start

### 1) Dry run
```bash
python3 -m downloads_organizer --root /mnt/c/Users/Juswaldy.Jusman/Downloads --dry-run --step all
```

### 2) Run for real
```bash
python3 -m downloads_organizer --root /mnt/c/Users/Juswaldy.Jusman/Downloads --step all
```

### 3) Run one step at a time
```bash
python3 -m downloads_organizer --root /mnt/c/Users/Juswaldy.Jusman/Downloads --dry-run --step 1
python3 -m downloads_organizer --root /mnt/c/Users/Juswaldy.Jusman/Downloads --dry-run --step 2
# ...
```

## Notes
- Heuristics are conservative and designed for *Downloads-style* mess; you can extend keyword lists in `downloads_organizer/rules.py`.
- If a destination name collides, the app auto-renames by appending `__1`, `__2`, etc.
