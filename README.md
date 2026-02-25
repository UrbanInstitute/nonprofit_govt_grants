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

# Known Technical Issues

## Roundtripping `sf` Objects Through `data.table`

`sf` stores spatial metadata (geometry column name, CRS) as R attributes on the data frame. `data.table` does not preserve these attributes through its operations, so naively converting between the two formats will corrupt the `sf` object. This means `data.table` should be treated as a **transit format only** — use it for I/O performance, but always land back on a plain `data.frame` before handing off to `sf`.

### Procedure

#### Writing
```r
# Step 1: Convert sf to data.table
# Drops sf class and attributes — this is intentional since we are serializing to disk
bmf_dt <- data.table::as.data.table(bmf_sf)

# Step 2: Serialize geometry column to WKT (Well-Known Text)
# The geometry column is an sfc list-column that fwrite() cannot serialize.
# digits = 15 preserves full floating-point precision — without this, 
# coordinates are rounded and downstream spatial joins may be affected.
bmf_dt[, geometry := sf::st_as_text(geometry, digits = 15)]

# Step 3: Write to disk
data.table::fwrite(bmf_dt, "output.csv")
```

#### Reading
```r
# Step 1: Read from disk
# geometry column is read back as plain character strings of WKT
bmf_read <- data.table::fread("output.csv")

# Step 2: Convert to plain data.frame before calling sf
# Critical step — sf sets its metadata as R attributes, which data.table 
# strips through its operations. A plain data.frame gives sf a stable 
# base to attach attributes to, preventing the error:
# "attr(obj, 'sf_column') does not point to a geometry column"
bmf_sf <- bmf_read |>
  as.data.frame() |>
  sf::st_as_sf(wkt = "geometry", crs = 4326)
```

### Common Errors

**`Error in st_geometry.sf(x): attr(obj, "sf_column") does not point to a geometry column`**
You called `st_as_sf()` on a `data.table` object. Insert `as.data.frame()` before `st_as_sf()`.

**OGR / WKB parsing error when reading back**
Do not use `sf::st_as_binary()` for CSV serialization. Hex WKB cannot be read back via `st_as_sf(wkt = ...)`. Use `sf::st_as_text(digits = 15)` instead.
