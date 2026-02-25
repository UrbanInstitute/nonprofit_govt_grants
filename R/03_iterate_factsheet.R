# Script Header
# Title: Federal Funding Freeze Blog Post
# Date created: 2025-02-03
# Date last modified: 2025-02-20
# Description: This script iterates across the national_factsheet.Rmd and
# state_factsheet.Rmd files to create HTML fact sheets for the US and each
# state respectively. Supports multiple tax years.

# ==============================================================================
# PACKAGES
# ==============================================================================

library(rmarkdown)
library(stringr)
library(tidyverse)
library(janitor)
library(usdata)

# ==============================================================================
# CONFIGURATION
# ==============================================================================

source("R/config.R")

#' Render a factsheet from an Rmd template
render_factsheet <- function(template, output_dir, output_file, params) {
  rmarkdown::render(
    input = template,
    output_dir = output_dir,
    output_file = output_file,
    params = params
  )
}

# ==============================================================================
# RENDER FUNCTION
# ==============================================================================

#' Render all factsheets for a single tax year
#'
#' Renders the national factsheet and all 51 state factsheets to the
#' year-specific docs directory.
#'
#' @param year Integer tax year (e.g. 2021L)
render_year <- function(year) {
  cat("\n========================================\n")
  cat("Rendering factsheets for year:", year, "\n")
  cat("========================================\n\n")

  output_dir <- dir_docs_year(year)
  dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

  # Copy CSS to output directory (Rmd uses relative path to web_report.css)
  file.copy("R/web_report.css", file.path(output_dir, "web_report.css"),
            overwrite = TRUE)

  # (1) Render national factsheet
  cat("Rendering national factsheet\n")
  render_factsheet(
    template = "R/national_factsheet.Rmd",
    output_dir = output_dir,
    output_file = "national.html",
    params = list(geography = "United States", year = year)
  )

  # (2) Render state factsheets
  for (state in STATE_NAMES) {
    cat("Rendering", state, "\n")
    render_factsheet(
      template = "R/state_factsheet.Rmd",
      output_dir = output_dir,
      output_file = paste0(gsub(" ", "-", tolower(state)), ".html"),
      params = list(state = state, year = year)
    )
  }

  cat("\n== Year", year, "rendering complete ==\n")
  cat("  Output directory:", output_dir, "\n")
}

# ==============================================================================
# STANDALONE EXECUTION
# ==============================================================================

if (!exists(".PIPELINE_ORCHESTRATED")) {
  render_year(2021)
}
