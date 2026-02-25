# Script Header
# Title: Federal Funding Freeze Blog Post
# Date created: 2025-02-03
# Date last modified: 2025-03-07
# Description: This script contains code to analyze data for HTML fact sheets
# on nonprofits's fiscal sustainability and reliance on government grants.
# Supports multiple tax years. Creates data.frames for fact sheets for each
# disaggregation.
# Detailed Description
# (1) - Create national level tables
# (2) - Create state level tables
# (3) - Create quality assurance tables for internal team viewing (optional)

# ==============================================================================
# PACKAGES
# ==============================================================================

library(tidyverse)
library(data.table)
library(rlang)
library(janitor)
library(writexl)

# ==============================================================================
# CONFIGURATION AND HELPERS
# ==============================================================================

source("R/config.R")
source("R/summarize_data.R")
source("R/retrieve_missing_counties.R")

# ==============================================================================
# STATE SUMMARIES FUNCTION
# ==============================================================================

#' Create and save summary datasets for a single state
#'
#' @param state State name
#' @param full_sample_proc Processed sample data frame
#' @param national National-level summary
#' @param absent_counties Absent counties data frame
#' @param output_dir_factsheets Directory for state CSV files
#' @param output_dir_overviews Directory for state .xlsx files
create_state_summaries <- function(state, full_sample_proc, national,
                                   absent_counties,
                                   output_dir_factsheets = DIR_STATE_FACTSHEETS,
                                   output_dir_overviews = DIR_STATE_OVERVIEWS) {
  state_sample <- full_sample_proc |>
    dplyr::filter(CENSUS_STATE_NAME == state)

  missing_county <- retrieve_missing_counties(absent_counties, state = state)

  state_overall <- summarize_nonprofit_data(state_sample)

  state_bycounty <- dplyr::mutate(national, Geography = "United States") |>
    dplyr::bind_rows(dplyr::mutate(state_overall, Geography = state)) |>
    dplyr::bind_rows(
      summarize_nonprofit_data(state_sample,
                               group_var = "CENSUS_COUNTY_NAME",
                               group_var_rename = "Geography")
    ) |>
    dplyr::relocate(Geography)

  state_bydistrict <- dplyr::mutate(national, Geography = "United States") |>
    dplyr::bind_rows(dplyr::mutate(state_overall, Geography = state)) |>
    dplyr::bind_rows(
      summarize_nonprofit_data(state_sample,
                               group_var = "CONGRESS_DISTRICT_NAME",
                               group_var_rename = "Geography")
    ) |>
    dplyr::relocate(Geography)

  state_bysize <- summarize_nonprofit_data(state_sample,
                                           group_var = "EXPENSE_CATEGORY",
                                           group_var_rename = "Size") |>
    dplyr::bind_rows(dplyr::mutate(state_overall, Size = "Total"))

  state_bysubsector <- summarize_nonprofit_data(state_sample,
                                                group_var = "SUBSECTOR",
                                                group_var_rename = "Subsector") |>
    dplyr::bind_rows(dplyr::mutate(state_overall, Subsector = "Total"))

  # Save CSVs
  state_slug <- gsub(" ", "-", tolower(state))
  datasets <- list(
    "_bycounty" = state_bycounty,
    "_bydistrict" = state_bydistrict,
    "_bysize" = state_bysize,
    "_bysubsector" = state_bysubsector
  )

  purrr::walk2(datasets, names(datasets), function(df, name) {
    data.table::fwrite(df, file.path(output_dir_factsheets,
                                     paste0(state_slug, name, ".csv")))
  })

  # Save .xlsx overview
  writexl::write_xlsx(
    list(
      "Overall" = state_overall,
      "County" = state_bycounty,
      "Congressional District" = state_bydistrict,
      "Size" = state_bysize,
      "Subsector" = state_bysubsector
    ),
    path = file.path(output_dir_overviews, paste0(state_slug, "_overview.xlsx"))
  )
}

# ==============================================================================
# MAIN ANALYSIS FUNCTION
# ==============================================================================

#' Run analysis for a single tax year
#'
#' Reads processed data, creates national and state summaries, generates QA
#' tables, and saves all outputs to year-specific directories.
#'
#' @param year Integer tax year (e.g. 2021L)
analyze_year <- function(year) {
  cat("\n========================================\n")
  cat("Analyzing year:", year, "\n")
  cat("========================================\n\n")

  # Year-specific output directories
  out_processed   <- dir_processed_year(year)
  out_factsheets  <- dir_state_factsheets_year(year)
  out_overviews   <- dir_state_overviews_year(year)
  out_intermediate <- dir_intermediate_year(year)

  dir.create(out_factsheets, showWarnings = FALSE, recursive = TRUE)
  dir.create(out_overviews, showWarnings = FALSE, recursive = TRUE)
  dir.create(out_intermediate, showWarnings = FALSE, recursive = TRUE)

  # ============================================================================
  # LOAD DATA
  # ============================================================================

  full_sample_proc <- data.table::fread(processed_data_file(year))
  absent_counties <- data.table::fread(intermediate_absent_counties_file(year))

  # ============================================================================
  # (1) NATIONAL SUMMARIES
  # ============================================================================

  cat("== Creating national summaries ==\n")

  national <- summarize_nonprofit_data(full_sample_proc)

  national_bystate <- dplyr::mutate(national, State = "United States") |>
    dplyr::bind_rows(
      summarize_nonprofit_data(
        full_sample_proc,
        group_var = "CENSUS_STATE_NAME",
        group_var_rename = "State"
      )
    ) |>
    dplyr::relocate(State)

  national_bysize <- summarize_nonprofit_data(full_sample_proc,
                                              group_var = "EXPENSE_CATEGORY",
                                              group_var_rename = "Size") |>
    dplyr::bind_rows(dplyr::mutate(national, Size = "Total"))

  national_bysubsector <- summarize_nonprofit_data(full_sample_proc,
                                                   group_var = "SUBSECTOR",
                                                   group_var_rename = "Subsector") |>
    dplyr::bind_rows(dplyr::mutate(national, Subsector = "Total"))

  # Save datasets
  datasets <- list(
    "national_bystate" = national_bystate,
    "national_bysize" = national_bysize,
    "national_bysubsector" = national_bysubsector
  )

  purrr::walk2(
    datasets,
    names(datasets),
    function(df, name) {
      data.table::fwrite(df, file.path(out_processed, paste0(name, ".csv")))
    }
  )

  writexl::write_xlsx(list(
    "National" = national,
    "State" = national_bystate,
    "Size" = national_bysize,
    "Subsector" = national_bysubsector
  ), path = file.path(out_processed, "national_overview.xlsx"))

  # ============================================================================
  # (2) STATE TABLES
  # ============================================================================

  cat("== Creating state summaries ==\n")

  for (state in STATE_NAMES) {
    cat("Processing ", state, "\n")
    create_state_summaries(state, full_sample_proc, national, absent_counties,
                           output_dir_factsheets = out_factsheets,
                           output_dir_overviews = out_overviews)
  }

  # ============================================================================
  # (3) QUALITY ASSURANCE (optional, internal)
  # ============================================================================

  cat("== Creating QA tables ==\n")

  qa <- full_sample_proc |>
    dplyr::mutate(state_name = CENSUS_STATE_NAME)

  qa_national <- summarize_nonprofit_data(qa) |>
    dplyr::mutate(CENSUS_STATE_NAME = "Total")

  qa_state <- summarize_nonprofit_data(qa,
                                       group_var = "state_name",
                                       group_var_rename = "State",
                                       qa = TRUE) |>
    dplyr::bind_rows(dplyr::mutate(qa_national, State = "Total"))

  qa_district <- summarize_nonprofit_data(qa,
                                          group_var = "CONGRESS_DISTRICT_NAME",
                                          group_var_rename = "Congressional District",
                                          qa = TRUE) |>
    dplyr::bind_rows(dplyr::mutate(qa_national, State = "Total"))

  missing_counties <- retrieve_missing_counties(absent_counties)

  qa_county <- summarize_nonprofit_data(qa,
                                        group_var = "CENSUS_COUNTY_NAME",
                                        group_var_rename = "County",
                                        qa = TRUE) |>
    dplyr::bind_rows(missing_counties) |>
    dplyr::bind_rows(dplyr::mutate(qa_national, County = "Total"))

  qa_size <- summarize_nonprofit_data(qa,
                                      group_var = "EXPENSE_CATEGORY",
                                      group_var_rename = "Size",
                                      qa = TRUE) |>
    dplyr::bind_rows(dplyr::mutate(qa_national, Size = "Total"))

  qa_subsector <- summarize_nonprofit_data(full_sample_proc,
                                           group_var = "SUBSECTOR",
                                           group_var_rename = "Subsector",
                                           qa = TRUE) |>
    dplyr::bind_rows(dplyr::mutate(qa_national, Subsector = "Total"))

  writexl::write_xlsx(list(
    "State" = qa_state,
    "County" = qa_county,
    "Congressional District" = qa_district,
    "Size" = qa_size,
    "Subsector" = qa_subsector
  ), path = file.path(out_intermediate, "qa.xlsx"))

  cat("\n== Year", year, "analysis complete ==\n")
}

# ==============================================================================
# STANDALONE EXECUTION
# ==============================================================================

if (!exists(".PIPELINE_ORCHESTRATED")) {
  analyze_year(2021)
}
