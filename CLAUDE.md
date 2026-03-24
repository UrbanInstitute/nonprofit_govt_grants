# CLAUDE.md — Project Context for Claude Code

## What This Project Is

An R data pipeline that produces **52 interactive HTML factsheets** (1 national + 51 state/DC) analyzing the financial risk to nonprofits if they lost government grants. Published as Urban Institute's [interactive data tool](https://www.urban.org/research/publication/what-financial-risk-nonprofits-losing-government-grants).

The unit of analysis is a **501(c)(3) public charity** that electronically filed IRS Form 990 and reported receiving government grants. The pipeline supports **tax years 2021, 2022, and 2023** (`SUPPORTED_YEARS` in config). Outputs are organized into year-specific subdirectories.

## Pipeline Execution Order

### Orchestrated (recommended)
```
Rscript R/run_pipeline.R              # All 3 years (2021, 2022, 2023)
Rscript R/run_pipeline.R --year 2022  # Single year
```

### Individual scripts (standalone, defaults to TY2021)
```
R/00_download.R          → Downloads raw data (~1.5 GB BMF + year-specific efile CSVs)
R/01_data_process.R      → Cleans, deduplicates, computes metrics → data/processed/{year}/full_sample_processed_v1.0.csv
R/validate_processed_data.R → Validates processed data integrity (called by run_pipeline.R)
R/02_analysis.R          → Aggregates into national + 51 state summary CSVs
R/03_iterate_factsheet.R → Renders national_factsheet.Rmd and state_factsheet.Rmd → docs/{year}/*.html
```

All pipeline scripts `source("R/config.R")` for shared constants. Run them from the project root (the working directory must be the repo root). Each script has a standalone guard that defaults to TY2021 when run outside the orchestrator.

### Summary report (standalone)
```
R/04_summary_report.qmd  → Cross-year trends report (Quarto → DOCX, requires `quarto render`)
```
This is **not** part of the automated pipeline. It reads processed data from all three years and produces a Word document comparing trends. Additional dependencies: `patchwork`, `kableExtra`.

## Key Architecture Decisions

- **`R/config.R`** is the single source of truth for all paths, URLs, column specs, factor levels, subsector mappings, expense breaks, census regions, and state vectors. It also defines year-aware path functions (`dir_processed_year()`, `processed_data_file()`, etc.) and `build_efile_urls()` for multi-year support. No `library()` calls — purely constants and path functions.
- **`R/run_pipeline.R`** is the orchestration entry point. Sets `.PIPELINE_ORCHESTRATED <- TRUE` to suppress standalone auto-execution in each script. Loads shared reference data (BMF, TIGRIS, foreign nonprofits) once and passes it to each year's processing. Uses `tryCatch` per year so one failure doesn't halt others.
- **Processed data version** is `v1.0` (defined as `PROCESSED_DATA_VERSION` in config). The filename `full_sample_processed_v1.0.csv` is canonical. Never change this without updating config.
- **Data request scripts** in `R/data_requests/` are **frozen and date-stamped** for reproducibility. Do not refactor them to use shared helpers. They may duplicate logic from the main pipeline intentionally.
- **Helper functions** (`cash_on_hand.R`, `operating_reserve_ratio.R`, `proportion_govt_grant.R`) are unused by the pipeline but kept for potential future use.
- **E-file parts**: Only 4 parts are used (P00 Header, P01 Summary, P08 Revenue, P09 Expenses). P05 (Other IRS Filing) and P10 (Balance Sheet) were dropped as unused in calculations.

## Directory Structure

```
R/
  config.R                  # All constants, paths, mappings, URLs, year-aware functions
  run_pipeline.R            # Orchestration entry point (multi-year)
  00_download.R             # Download orchestration: download_year(year)
  01_data_process.R         # Data processing: process_year(year, ref_data)
  validate_processed_data.R # Validation: validate_processed_data(file, year)
  02_analysis.R             # Analysis: analyze_year(year)
  03_iterate_factsheet.R    # Rendering: render_year(year)
  national_factsheet.Rmd    # Template for national HTML factsheet (year param)
  state_factsheet.Rmd       # Template for 51 state HTML factsheets (year param)
  summarize_data.R          # Core: summarize_nonprofit_data()
  deduplicate_returns.R     # Helper: deduplicate group/amended returns
  impute_missing_geography.R # Helper: spatial join to fill missing geo fields
  format_gt_table.R         # Helper: shared GT table styling for Rmd
  download_data.R           # Helper: download_files() with error handling
  profit_margin.R           # profit_margin() (scalar) + profit_margin_vec() (vectorized)
  format_ein.R              # EIN formatting (XX-XXXXXXX ↔ numeric)
  format_districts.R        # Congressional district name formatting (handles 0, 1, 2, 3+ districts)
  format_percentages.R      # Percentage string → numeric conversion
  retrieve_missing_counties.R # Counties with 0 nonprofits receiving govt grants
  create_sorted_plot.R      # Exploratory sorted scatter plots (interactive only)
  04_summary_report.qmd     # Standalone: cross-year trends report (Quarto → DOCX)
  cash_on_hand.R            # Unused — days/months of cash metric
  operating_reserve_ratio.R # Unused — operating reserve ratio metric
  proportion_govt_grant.R   # Unused — govt grant proportion metric
  sfchronicle-06102025.R    # Frozen data request
  data_requests/            # Frozen, date-stamped ad-hoc analyses
    congressional_briefing-20250715.R        # NY/FL/PR + demographics
    congressional_briefing_utils-20250715.R  # Helpers for congressional briefing
    municipal_innovations-20250724.R         # 20 cities × 5 policy areas
    municipal_innovations_utils-20250724.R   # Helpers for municipal innovations
    municipal_innovations_tests-20250725.R   # Tests for municipal innovations
    municipal_innovations_city_factsheet-20250724.Rmd   # City factsheet template
    municipal_innovations_city_factsheet-20250724.docx  # Rendered output

data/
  raw/
    unified_bmf.csv                   # Shared BMF (~1.5 GB)
    foreign_nonprofits.csv            # Shared foreign nonprofits list
    {year}/                           # Per-year efile downloads
      efile_p00.csv, efile_p01.csv, efile_p08.csv, efile_p09.csv
  intermediate/
    bmf_sample.csv                    # Shared wrangled BMF
    qa_all_years.xlsx                 # Consolidated QA across years
    {year}/
      full_sample.csv, absent_counties.csv, qa.xlsx
  processed/
    full_sample_processed_v1.0.csv    # Legacy TY2021 (root level)
    national_by{state,size,subsector}.csv  # Legacy TY2021 (root level)
    state_factsheets/                 # Legacy TY2021 state CSVs
    {year}/
      full_sample_processed_v1.0.csv
      national_bystate.csv, national_bysize.csv, national_bysubsector.csv
      national_overview.xlsx
      state_factsheets/               # 51 × 4 CSVs
      state_overviews/                # 51 .xlsx files

docs/
  *.html                              # Legacy TY2021 factsheets (root level)
  {year}/
    national.html, alabama.html, ... wyoming.html  # self-contained (CSS/JS embedded)
```

## Data Sources

All URLs are defined in `R/config.R`:
- **E-file data** (4 parts per year): IRS Form 990 Parts Header (P00), Summary (P01), Revenue (P08), Expenses (P09). Built dynamically by `build_efile_urls(year)` using the `efile_v2_1` S3 base URL.
- **Unified BMF**: NCCS Business Master File V1.1 (~1.5 GB) — shared across years
- **Foreign nonprofits**: IRS SOI eo_xx.csv (used to exclude non-US orgs) — shared across years
- **TIGRIS**: US Census state, county, and congressional district shapefiles (fetched via `tigris` R package at runtime, not stored)

## Key Columns in the Processed Dataset

| Column | Description |
|--------|-------------|
| `EIN2` | Formatted EIN (EIN-XX-XXXXXXX) |
| `CENSUS_STATE_NAME` | Full state name |
| `CENSUS_COUNTY_NAME` | County name |
| `CONGRESS_DISTRICT_NAME` | E.g. "1st Congressional district" |
| `SUBSECTOR` | Factor: 12 levels (Arts, Education, Health, etc.) |
| `EXPENSE_CATEGORY` | Factor: 6 size buckets by total expenses |
| `GOVERNMENT_GRANT_DOLLAR_AMOUNT` | Part VIII Line 1e |
| `PROFIT_MARGIN` | (Revenue - Expenses) / Revenue |
| `PROFIT_MARGIN_NOGOVTGRANT` | (Revenue - Expenses - GovGrants) / Revenue |
| `AT_RISK_NUM` | Binary: 1 if PROFIT_MARGIN_NOGOVTGRANT < 0 |

## Important Patterns

- **Factor levels** for subsectors, expense categories, and congressional districts are defined in `config.R` (`SUBSECTOR_LEVELS`, `EXPENSE_LABELS`, `CONGRESS_DISTRICT_LEVELS`). Both Rmd templates reference these. If you add/change a level, update it in config.
- **`summarize_nonprofit_data()`** in `R/summarize_data.R` is the canonical aggregation function used everywhere. It groups, summarizes, formats, and renames columns. It has a `qa` mode for internal QA datasets.
- **GT table styling** uses `style_factsheet_table()` from `R/format_gt_table.R` in both Rmd files.
- **`profit_margin_vec()`** is the vectorized version of `profit_margin()` — always prefer the vectorized version for column operations. Both profit margin calculations now use the vectorized version.
- **`format_districts()`** in `R/format_districts.R` handles all district counts: 0 (at-large only → empty string), 1 (singular "district"), 2 ("X and Y districts"), 3+ (Oxford comma list). The `make_ordinal()` helper uses `n %% 10 + 1L` offset to correctly handle multiples of 10 (10th, 20th, etc.) and checks `n %% 100` for 11th/12th/13th.
- **County suffix stripping** in `state_factsheet.Rmd` uses `strip_county_suffix()` to remove all county-equivalent geographic suffixes (County, Parish, Borough, Census Area, Municipality, city, City and Borough) — not just "County". This is needed for Alaska (Borough/Municipality/Census Area) and Louisiana (Parish).
- **Both Rmd templates require `library(epoxy)`** for the `{epoxy}` chunk engine. If the package is not loaded, the intro paragraph chunk is silently skipped.
- **Standalone guards**: Each pipeline script checks `if (!exists(".PIPELINE_ORCHESTRATED"))` and runs for TY2021 by default. This preserves backward compatibility for interactive/standalone use.

## R Package Dependencies

Core: `tidyverse`, `data.table`, `dtplyr`, `sf`, `tigris`, `lubridate`, `tidylog`, `usdata`, `rio`, `scales`
Analysis: `rlang`, `janitor`, `writexl`, `readxl`
Rendering: `rmarkdown`, `gt`, `gtExtras`, `urbnthemes`, `epoxy`, `glue`, `rprojroot`, `stringr`
Summary report only: `patchwork`, `kableExtra`, `quarto` (CLI)

## Common Tasks

- **Run full pipeline for all years**: `Rscript R/run_pipeline.R`
- **Run pipeline for one year**: `Rscript R/run_pipeline.R --year 2022`
- **Re-render all factsheets for one year**: `source("R/03_iterate_factsheet.R")` (defaults to 2021) or call `render_year(2022)` after sourcing config
- **Re-render one state**: Use `render_factsheet()` from `03_iterate_factsheet.R` with specific params
- **Update a metric or column**: Change in `01_data_process.R`, re-run pipeline from that point
- **Add a new disaggregation**: Add to `02_analysis.R`'s `create_state_summaries()` and update the Rmd templates
- **Render summary report**: `quarto render R/04_summary_report.qmd` (standalone, not part of pipeline)

## Things to Watch Out For

- The `data/raw/` directory is **not in git** — it must be populated by running `R/00_download.R` or the full pipeline first.
- The BMF download is ~1.5 GB and may timeout — `download_data.R` has a 600-second timeout.
- `01_data_process.R` uses `tidylog` which prints verbose join diagnostics — this is intentional for QA.
- TIGRIS data is fetched live from Census servers each run; if their API is down, `load_reference_data()` will fail.
- The `rowwise()` bottleneck for `profit_margin` has been replaced by `profit_margin_vec()`. Both profit margin calculations now use the vectorized version.
- Legacy root-level outputs (`data/processed/*.csv`, `docs/*.html`) are preserved for backward compatibility with the published TY2021 tool. New outputs go to year-specific subdirectories.
- **DC duplicate county row**: DC's county CSV has a duplicate "District of Columbia" row. `state_factsheet.Rmd` uses `dplyr::distinct()` as a workaround. This is a known data quirk, not a bug in the rendering code.
