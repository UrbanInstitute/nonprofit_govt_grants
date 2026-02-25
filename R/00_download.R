# ==============================================================================
# Download Orchestration: Fetch raw data files for the pipeline
# ==============================================================================
# Run this script before 01_data_process.R to download raw data.
# Skips files that already exist unless overwrite = TRUE.
#
# Usage:
#   Standalone: source("R/00_download.R")  — downloads TY2021 by default
#   Orchestrated: download_year(year) is called by R/run_pipeline.R

source("R/config.R")
source("R/download_data.R")

#' Download raw data for a single tax year
#'
#' Creates year-specific directories and downloads efile data. Shared files
#' (BMF, foreign nonprofits) are downloaded to DIR_RAW (idempotent).
#'
#' @param year Integer tax year (e.g. 2021L)
download_year <- function(year) {
  cat("== Downloading data for year", year, "==\n")

  # Create directories
  dir.create(DIR_RAW, recursive = TRUE, showWarnings = FALSE)
  dir.create(dir_raw_year(year), recursive = TRUE, showWarnings = FALSE)
  dir.create(DIR_INTERMEDIATE, recursive = TRUE, showWarnings = FALSE)
  dir.create(dir_intermediate_year(year), recursive = TRUE, showWarnings = FALSE)
  dir.create(dir_processed_year(year), recursive = TRUE, showWarnings = FALSE)

  # Download shared data (BMF + foreign nonprofits) — idempotent

  download_files(BMF_URLS, DIR_RAW)
  download_files(XX_URLS, DIR_RAW)

  # Download year-specific efile data
  efile_urls <- build_efile_urls(year)
  download_files(efile_urls, dir_raw_year(year))
}

# Standalone execution guard
if (!exists(".PIPELINE_ORCHESTRATED")) {
  download_year(2021)
}
