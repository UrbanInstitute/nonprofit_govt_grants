# CLAUDE.md — Project Context for Claude Code

## What This Project Is

An R data pipeline that produces **52 interactive HTML factsheets** (1 national + 51 state/DC) analyzing the financial risk to nonprofits if they lost government grants. Published as Urban Institute's [interactive data tool](https://www.urban.org/research/publication/what-financial-risk-nonprofits-losing-government-grants).

The unit of analysis is a **501(c)(3) public charity** that electronically filed IRS Form 990 and reported receiving government grants in **tax year 2021**. The final sample contains ~103,475 returns.

## Pipeline Execution Order

```
R/00_download.R        → Downloads raw data (~1.5 GB BMF + efile CSVs)
R/00_data_process.R    → Cleans, deduplicates, computes metrics → data/processed/full_sample_processed_v1.0.csv
R/01_analysis.R        → Aggregates into national + 51 state summary CSVs
R/02_iterate_factsheet.R → Renders national_factsheet.Rmd and state_factsheet.Rmd → docs/*.html
```

All three numbered scripts `source("R/config.R")` for shared constants. Run them from the project root (the working directory must be the repo root).

## Key Architecture Decisions

- **`R/config.R`** is the single source of truth for all paths, URLs, column specs, factor levels, subsector mappings, expense breaks, census regions, and state vectors. Every pipeline script and both Rmd templates source it. It contains no `library()` calls — purely constants.
- **Processed data version** is `v1.0` (defined as `PROCESSED_DATA_VERSION` in config). The filename `full_sample_processed_v1.0.csv` is canonical. Never change this without updating config.
- **Data request scripts** in `R/data_requests/` are **frozen and date-stamped** for reproducibility. Do not refactor them to use shared helpers. They may duplicate logic from the main pipeline intentionally.
- **Helper functions** (`cash_on_hand.R`, `operating_reserve_ratio.R`, `proportion_govt_grant.R`) are unused by the pipeline but kept for potential future use.

## Directory Structure

```
R/
  config.R                  # All constants, paths, mappings, URLs
  00_download.R             # Download orchestration (run first)
  00_data_process.R         # Data processing pipeline (~473 lines)
  01_analysis.R             # Aggregation and summary tables (~231 lines)
  02_iterate_factsheet.R    # Rmd rendering loop (~61 lines)
  national_factsheet.Rmd    # Template for national HTML factsheet
  state_factsheet.Rmd       # Template for 51 state HTML factsheets
  summarize_data.R          # Core: summarize_nonprofit_data()
  deduplicate_returns.R     # Helper: deduplicate group/amended returns
  impute_missing_geography.R # Helper: spatial join to fill missing geo fields
  format_gt_table.R         # Helper: shared GT table styling for Rmd
  download_data.R           # Helper: download_files() with error handling
  profit_margin.R           # profit_margin() (scalar) + profit_margin_vec() (vectorized)
  format_ein.R              # EIN formatting (XX-XXXXXXX ↔ numeric)
  format_districts.R        # Congressional district name formatting
  format_percentages.R      # Percentage string → numeric conversion
  retrieve_missing_counties.R # Counties with 0 nonprofits receiving govt grants
  create_sorted_plot.R      # Exploratory sorted scatter plots (interactive only)
  cash_on_hand.R            # Unused — days/months of cash metric
  operating_reserve_ratio.R # Unused — operating reserve ratio metric
  proportion_govt_grant.R   # Unused — govt grant proportion metric
  sfchronicle-06102025.R    # Frozen data request
  data_requests/            # Frozen, date-stamped ad-hoc analyses

data/
  raw/                      # Downloaded CSVs (not in git, ~1.5 GB total)
  intermediate/             # BMF sample, absent counties, full intermediate sample
  processed/                # Final CSVs consumed by Rmd templates
    full_sample_processed_v1.0.csv  # The canonical processed dataset
    national_by{state,size,subsector}.csv
    state_factsheets/       # 51 × 4 CSVs (county, district, size, subsector)

docs/                       # 52 rendered HTML factsheets (national + 51 states)
```

## Data Sources

All URLs are defined in `R/config.R` under `EFILE_URLS`, `BMF_URLS`, `XX_URLS`:
- **E-file data** (6 files): IRS Form 990 Parts Header, I, V, VIII, IX, X for TY2021 from NCCS S3
- **Unified BMF**: NCCS Business Master File V1.1 (~1.5 GB)
- **Foreign nonprofits**: IRS SOI eo_xx.csv (used to exclude non-US orgs)
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
- **`profit_margin_vec()`** is the vectorized version of `profit_margin()` — always prefer the vectorized version for column operations. The scalar version is kept for `pmap_dbl` compatibility.

## R Package Dependencies

Core: `tidyverse`, `data.table`, `dtplyr`, `sf`, `tigris`, `lubridate`, `tidylog`, `usdata`, `rio`, `scales`
Analysis: `rlang`, `janitor`, `writexl`
Rendering: `rmarkdown`, `gt`, `gtExtras`, `urbnthemes`, `epoxy`, `glue`, `rprojroot`

## Common Tasks

- **Re-render all factsheets**: `source("R/02_iterate_factsheet.R")` (takes several minutes)
- **Re-render one state**: Use the render call from `02_iterate_factsheet.R` with a single state name
- **Update a metric or column**: Change in `00_data_process.R`, re-run pipeline from that point
- **Add a new disaggregation**: Add to `01_analysis.R`'s `create_state_summaries()` and update the Rmd templates

## Things to Watch Out For

- The `data/raw/` directory is **not in git** — it must be populated by running `R/00_download.R` first.
- The BMF download is ~1.5 GB and may timeout — `download_data.R` has a 600-second timeout.
- `00_data_process.R` uses `tidylog` which prints verbose join diagnostics — this is intentional for QA.
- TIGRIS data is fetched live from Census servers each run; if their API is down, steps 1.2 in `00_data_process.R` will fail.
- The `rowwise()` bottleneck for `profit_margin` has been replaced by `profit_margin_vec()`. If you see `rowwise()` in old code, it's a sign of pre-refactor code.
