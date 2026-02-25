# Financial Risk of Nonprofits Losing Government Grants

This repository contains the replication code, datasets and workflow used in Urban's [interactive data tool](https://www.urban.org/research/publication/what-financial-risk-nonprofits-losing-government-grants) on the financial risk of nonprofits losing government grants.

## Quick Start

```bash
# Run the full pipeline for all supported years (2021, 2022, 2023)
Rscript R/run_pipeline.R

# Run for a single year
Rscript R/run_pipeline.R --year 2022
```

The pipeline can also be run step-by-step (defaults to TY2021 when run standalone):

```
R/00_download.R          → Downloads raw data (~1.5 GB BMF + year-specific efile CSVs)
R/01_data_process.R      → Cleans, deduplicates, computes metrics
R/02_analysis.R          → Aggregates into national + 51 state summary CSVs
R/03_iterate_factsheet.R → Renders HTML factsheets
```

## Directory Descriptions

### /data
- **raw/**: Contains original, unmodified data files. These should never be altered once added.
    - This directory is not included in the repository due to the size of the files. To download the raw data, run `Rscript R/run_pipeline.R` or `source("R/00_download.R")`.
    - Shared files (`unified_bmf.csv`, `foreign_nonprofits.csv`) live at the root level.
    - Year-specific efile data lives in `raw/{year}/` subdirectories.
- **intermediate/**: Stores partially processed data files and temporary outputs from processing steps.
    - `bmf_sample.csv`: Wrangled BMF data (shared across years).
    - `{year}/qa.xlsx`: Year-specific QA datasets.
    - `qa_all_years.xlsx`: Consolidated QA across all years (created by `run_pipeline.R`).
- **processed/**: Contains final, cleaned datasets that are used in the factsheets.
    - `{year}/full_sample_processed_v1.0.csv`: Records for each return in the final sample.
    - `{year}/state_factsheets/`: State-level data disaggregated by county, district, size and subsector.
    - `{year}/state_overviews/`: State-level overview .xlsx files.
    - Root-level files are legacy TY2021 outputs preserved for backward compatibility.

### /R
- Contains all R code for data analysis, processing, and visualization. Scripts are numbered sequentially to indicate the order of execution. `R/run_pipeline.R` is the orchestration entry point that runs all steps for one or more years.
- Helper functions are stored in separate scripts named after their primary function.
- `national_factsheet.Rmd` and `state_factsheet.Rmd` are parameterized templates that accept a `year` parameter.

### /docs
- Contains the HTML factsheets generated from the data analysis.
- Year-specific outputs go to `docs/{year}/`.
- Root-level files are legacy TY2021 outputs.
