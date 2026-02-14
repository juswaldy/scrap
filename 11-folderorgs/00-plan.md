# Downloads folder organization plan

Date: 2026-01-23
Scope: `/mnt/c/Users/Juswaldy.Jusman/Downloads` (highly mixed: docs, datasets, code, installers, archives, media, diagrams, logs, backups).

## Classification strategy (how to decide where something goes)

### 1) Classify by **use** first, not file type
Ask in order:
1. **Is it a software artifact?** (installer, binary, VSIX, MSI, EXE, ISO) → *Software*.
2. **Is it code or a runnable project?** (folders like `app/`, `webapi/`, `cloverdx-*`, `.py/.rb/.ipynb/.sql` that form a set) → *Projects*.
3. **Is it a dataset or export?** (CSV/XLSX/JSON/XML dumps, “report”, “export”, “extract”, “load”, “hierarchy”, etc.) → *Data*.
4. **Is it a business/work document?** (requirements, decks, guides, contracts, forms, meeting notes) → *Work Docs*.
5. **Is it research/reading?** (papers, books, articles, PDFs not tied to current work deliverables) → *Reading*.
6. **Is it media?** (images/video/audio) → *Media*.
7. **Is it a system artifact / log / backup?** (`.bak`, `.zip` support bundles, logs, config backups) → *Ops & Backups*.
8. **Is it temporary/unknown?** → *Inbox / To-Sort*.

### 2) Then subgroup by **domain/topic** (finance, HR, housing, identity, integrations, AI/ML, etc.)
Use stable topic buckets that match how you search mentally (examples present in this folder):
- Finance / GL / Vena / AR-AP / Tuition / Trial Balance
- Student / Enrollment / Housing / Residence Life
- Identity / Entra / AD / Email / SSO
- Integrations / CloverDX / Jenzabar / Salesforce
- Governance / Strategy / Maturity Models
- AI/ML / Data Science / Papers

### 3) Then subgroup by **format/time/state**
- Format: `pdf`, `docx`, `pptx`, `xlsx`, `sql`, `ipynb`, etc. only when it helps.
- Time: `YYYY/`, `YYYY-MM/` or `2026-Q1/` when there are many versions.
- State: `draft/`, `final/`, `archive/`, `samples/`.

### 4) Naming conventions (keep it searchable)
- Prefer: `YYYY-MM-DD - Topic - Short title.ext`
- Avoid duplicate “final final”: use version suffix: `v1`, `v2`, `revA`, or Git for projects.
- For exports: `Source-System__Object__YYYYMMDD_HHMM.ext` (you already have many like this).

### 5) Rules to reduce clutter
- Anything in Downloads older than “active week” should be moved into the hierarchy.
- Large binaries/installers kept only if you actually need them; otherwise delete.
- Archives (`.zip/.7z/.tar.gz`) should be either:
  - extracted into a project folder and then archived to `Archives/`, or
  - kept as-is in `Archives/` with a short README note.

---

## Alternative hierarchy A: **Purpose-first** (recommended)
This is best when you want quick retrieval by intent (work vs reading vs software).

### Top-level folders
1. `00-Inbox/`  *(default landing zone; empty it weekly)*
2. `01-Work/`
3. `02-Projects/`
4. `03-Data/`
5. `04-Reading/`
6. `05-Media/`
7. `06-Software/`
8. `07-Ops-Backups/`
9. `99-Archive/`

### Example subgrouping (3 levels)

#### `01-Work/`
- `01-Work/Finance/`
  - `01-Work/Finance/Vena/` (e.g., *Vena users review*, mappings, errors)
  - `01-Work/Finance/Reporting/` (trial balance, financial reporting PDFs)
  - `01-Work/Finance/AR-AP/` (AR aging queries, positive pay docs)
- `01-Work/Student/`
  - `01-Work/Student/Housing/` (housing queries, diagrams)
  - `01-Work/Student/Enrollment/`
- `01-Work/Identity/`
  - `01-Work/Identity/Email/` (Institutional Email notebooks, docs)
  - `01-Work/Identity/Entra-AD/` (Entra YAML/exports)
- `01-Work/Integrations/`
  - `01-Work/Integrations/CloverDX/` (support packages, configs)
  - `01-Work/Integrations/Jenzabar/` (upgrade docs/scripts)
  - `01-Work/Integrations/Salesforce/` (adapter files, SOQL)

#### `02-Projects/`
- `02-Projects/Aqueduct/`
  - `02-Projects/Aqueduct/sql/`
  - `02-Projects/Aqueduct/docs/`
- `02-Projects/CloverDX/`
  - `02-Projects/CloverDX/graphs/`
  - `02-Projects/CloverDX/config/`
- `02-Projects/Personal/` (misc scripts, experiments)

#### `03-Data/`
- `03-Data/Finance/`
  - `03-Data/Finance/GL/` (CoA, hierarchies, mappings)
  - `03-Data/Finance/Reports/` (exports)
- `03-Data/Identity/` (user exports, groups)
- `03-Data/Student/` (housing/enrollment exports)
- `03-Data/Samples/` (templates)

#### `04-Reading/`
- `04-Reading/AI-ML/`
  - `04-Reading/AI-ML/Papers/`
  - `04-Reading/AI-ML/Books/`
- `04-Reading/Data-Governance/`
- `04-Reading/Security/`

#### `05-Media/`
- `05-Media/Images/` (PNG/JPG/WebP/SVG)
- `05-Media/Video/` (MP4/MOV)
- `05-Media/Audio/` (MP3/WAV)

#### `06-Software/`
- `06-Software/Installers/` (MSI/EXE)
- `06-Software/ISOs/`
- `06-Software/Drivers-SDKs/` (dotnet SDKs, etc.)

#### `07-Ops-Backups/`
- `07-Ops-Backups/DB-Backups/` (`.bak`, dacpac)
- `07-Ops-Backups/Logs/`
- `07-Ops-Backups/Support-Bundles/`
- `07-Ops-Backups/Certificates-Keys/` *(restrict permissions; consider moving out of Downloads)*

---

## Alternative hierarchy B: **Domain-first** (works better if most files are work-related)
This is best when nearly everything is related to a few big domains and you want all artifacts together.

### Top-level folders
1. `00-Inbox/`
2. `10-Finance/`
3. `20-Student/`
4. `30-Identity/`
5. `40-Integrations/`
6. `50-Data-Science-AI/`
7. `60-Operations/`
8. `70-Personal/`
9. `99-Archive/`

### Example subgrouping (3 levels)

#### `10-Finance/`
- `10-Finance/Docs/` (guides, PDFs, decks)
- `10-Finance/SQL/` (queries, scripts)
- `10-Finance/Data/` (CSV/XLSX exports)
- `10-Finance/Notebooks/` (ipynb)
- `10-Finance/Vena/` (anything Vena-specific)

#### `20-Student/`
- `20-Student/Housing/{Docs,SQL,Data,Diagrams}/`
- `20-Student/Enrollment/{Docs,SQL,Data}/`

#### `40-Integrations/`
- `40-Integrations/CloverDX/{Projects,Configs,Support-Bundles}/`
- `40-Integrations/Jenzabar/{Upgrade,SQL,Docs}/`
- `40-Integrations/Salesforce/{Exports,SOQL,Docs}/`

#### `50-Data-Science-AI/`
- `50-Data-Science-AI/Papers/`
- `50-Data-Science-AI/Notebooks/`
- `50-Data-Science-AI/Datasets/`

#### `60-Operations/`
- `60-Operations/Backups/`
- `60-Operations/Logs/`
- `60-Operations/Configs/`
- `60-Operations/Installers/` (if you prefer keeping software here)

---

## Decision guide: pick A vs B
- Choose **A (Purpose-first)** if you often think “is this work, reading, software, or media?” before you think topic.
- Choose **B (Domain-first)** if you usually think “finance vs student vs identity vs integrations” first and want every artifact type under that.

---

## Concrete next steps (safe, incremental)
1. Create the chosen top-level folders.
2. Move everything from the current root into `00-Inbox/` first (optional but reduces stress).
3. Sort `00-Inbox/` in passes:
   - Pass 1: big obvious buckets: `Media`, `Software`, `Archives`, `Backups`.
   - Pass 2: Work vs Reading.
   - Pass 3: Domain subfolders.
4. For “project-like” directories (`ClibBuilder/`, `Clover/`, `SalesforceAdapter/`, `webapi/`, etc.), move them intact under `02-Projects/` (A) or the appropriate domain (B).
5. For duplicates / versions, keep the newest in the main folder and move older into `99-Archive/<topic>/`.

## Notes specific to this folder
- You have many **finance + integration** artifacts and many **AI/ML PDFs**; both hierarchies handle that well.
- There appear to be keys/certs (e.g., `annotheta.pem`, `rsa_key.*`, `serverKS.jks`): consider moving to a secure location outside Downloads and tightening permissions.
