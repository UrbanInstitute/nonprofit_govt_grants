# ==============================================================================
# Download Orchestration: Fetch raw data files for the pipeline
# ==============================================================================
# Run this script before 00_data_process.R to download raw data.
# Skips files that already exist unless overwrite = TRUE.

source("R/config.R")
source("R/download_data.R")

# Create directories
dir.create(DIR_RAW, recursive = TRUE, showWarnings = FALSE)
dir.create(DIR_INTERMEDIATE, recursive = TRUE, showWarnings = FALSE)
dir.create(DIR_PROCESSED, recursive = TRUE, showWarnings = FALSE)

# Download efile data
download_files(EFILE_URLS, DIR_RAW)

# Download Unified BMF data (large file ~1.5 GB)
download_files(BMF_URLS, DIR_RAW)

# Download foreign nonprofits list
download_files(XX_URLS, DIR_RAW)
