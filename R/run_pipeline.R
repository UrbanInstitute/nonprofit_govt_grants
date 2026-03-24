# ==============================================================================
# Pipeline Orchestrator: Run the full pipeline for one or more tax years
# ==============================================================================
#
# Usage:
#   Rscript R/run_pipeline.R              # All supported years (2021, 2022, 2023)
#   Rscript R/run_pipeline.R --year 2022  # Single year
#
# Steps per year:
#   1. Download raw data
#   2. Process data (clean, deduplicate, compute metrics)
#   3. Validate processed data
#   4. Analyze data (national + state summaries)
#   5. Render HTML factsheets
#
# If one year fails, the error is logged and the pipeline continues to the next.

# ==============================================================================
# SETUP
# ==============================================================================

# Signal to all scripts that they are being orchestrated (suppress auto-execution)
.PIPELINE_ORCHESTRATED <- TRUE

library(readxl)

# Source config and all pipeline scripts (defines functions only)
source("R/config.R")
source("R/00_download.R")
source("R/01_data_process.R")
source("R/validate_processed_data.R")
source("R/02_analysis.R")
source("R/03_iterate_factsheet.R")

# ==============================================================================
# PARSE ARGUMENTS
# ==============================================================================

args <- commandArgs(trailingOnly = TRUE)

if ("--year" %in% args) {
  year_idx <- which(args == "--year")
  if (year_idx >= length(args)) {
    stop("--year flag requires a value (e.g. --year 2022)")
  }
  requested_years <- as.integer(args[year_idx + 1])
  if (is.na(requested_years)) {
    stop("Invalid year value: ", args[year_idx + 1])
  }
} else {
  requested_years <- SUPPORTED_YEARS
}

# Validate requested years
invalid_years <- setdiff(requested_years, SUPPORTED_YEARS)
if (length(invalid_years) > 0) {
  stop("Unsupported year(s): ", paste(invalid_years, collapse = ", "),
       ". Supported years: ", paste(SUPPORTED_YEARS, collapse = ", "))
}

cat("==============================================\n")
cat("  Pipeline: Processing years", paste(requested_years, collapse = ", "), "\n")
cat("==============================================\n\n")

# ==============================================================================
# LOAD SHARED REFERENCE DATA (once for all years)
# ==============================================================================

# Download shared files first (BMF + foreign nonprofits)
dir.create(DIR_RAW, recursive = TRUE, showWarnings = FALSE)
download_files(BMF_URLS, DIR_RAW)
download_files(XX_URLS, DIR_RAW)

cat("== Loading shared reference data ==\n")
ref_data <- load_reference_data()

# ==============================================================================
# RUN PIPELINE PER YEAR
# ==============================================================================

results <- list()

for (year in requested_years) {
  cat("\n##############################################\n")
  cat("  Starting pipeline for year:", year, "\n")
  cat("##############################################\n\n")

  results[[as.character(year)]] <- tryCatch({

    # Step 1: Download
    download_year(year)

    # Step 2: Process
    process_year(year, ref_data = ref_data)

    # Step 3: Validate
    validate_processed_data(processed_data_file(year), year)

    # Step 4: Analyze
    analyze_year(year)

    # Step 5: Render
    render_year(year)

    cat("\n== Year", year, "COMPLETED SUCCESSFULLY ==\n")
    list(status = "success", year = year)

  }, error = function(e) {
    cat("\n!! Year", year, "FAILED:", conditionMessage(e), "\n\n")
    list(status = "error", year = year, message = conditionMessage(e))
  })
}

# ==============================================================================
# CONSOLIDATE QA
# ==============================================================================

cat("\n== Consolidating QA across years ==\n")

qa_sheets <- list()
for (year in requested_years) {
  qa_file <- file.path(dir_intermediate_year(year), "qa.xlsx")
  if (file.exists(qa_file)) {
    year_sheets <- readxl::read_xlsx(qa_file, sheet = NULL)
    sheet_names <- readxl::excel_sheets(qa_file)
    for (sheet in sheet_names) {
      sheet_data <- readxl::read_xlsx(qa_file, sheet = sheet)
      qa_sheets[[paste(year, "-", sheet)]] <- sheet_data
    }
  }
}

if (length(qa_sheets) > 0) {
  writexl::write_xlsx(qa_sheets,
                      path = file.path(DIR_INTERMEDIATE, "qa_all_years.xlsx"))
  cat("  Saved:", file.path(DIR_INTERMEDIATE, "qa_all_years.xlsx"), "\n")
} else {
  cat("  No QA files found to consolidate\n")
}

# ==============================================================================
# SUMMARY
# ==============================================================================

cat("\n==============================================\n")
cat("  Pipeline Summary\n")
cat("==============================================\n")
for (r in results) {
  if (r$status == "success") {
    cat("  Year", r$year, ": SUCCESS\n")
  } else {
    cat("  Year", r$year, ": FAILED -", r$message, "\n")
  }
}
cat("==============================================\n")
