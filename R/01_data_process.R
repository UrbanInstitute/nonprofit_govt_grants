# Script Header
# Title: Federal Funding Freeze Blog Post
# Date created: 2025-01-31
# Date last modified: 2025-07-16
# Description: This script contains code to wrangle and process data for HTML
# fact sheets on nonprofits's fiscal sustainability and reliance on government
# grants. Supports multiple tax years (2021, 2022, 2023).
#
# Run R/00_download.R first to download raw data.
#
### Details:
# (1) - Load in and filter data
# (2) - Create the sample dataset from the efile data
# (3) - Wrangle Data
# (4) - Compute fiscal sustainability metrics
# (5) - Merge with the geographic data from the Unified BMF
# (6) - Geographic post processing
# (7) - Post process and save intermediate and processed sample datasets

# ==============================================================================
# PACKAGES
# ==============================================================================

library(rio)
library(data.table)
library(dtplyr)
library(tidyverse)
library(lubridate)
library(tidylog)
library(usdata)
library(sf)
library(tigris)

# ==============================================================================
# CONFIGURATION AND HELPERS
# ==============================================================================

source("R/config.R")
source("R/format_ein.R")
source("R/profit_margin.R")
source("R/create_sorted_plot.R")
source("R/deduplicate_returns.R")
source("R/impute_missing_geography.R")

# ==============================================================================
# (1) LOAD REFERENCE DATA (shared across years)
# ==============================================================================

#' Load reference data: BMF, TIGRIS shapefiles, foreign nonprofits
#'
#' These datasets are year-independent. When processing multiple years, call
#' this once and pass the result to process_year().
#'
#' @return Named list with: unified_bmf, state_tigris, county_tigris,
#'   cd_transformed, county_state, district_state, foreign_ein
load_reference_data <- function() {
  cat("== Loading reference data ==\n")

  # (1.1) BMF Data
  unified_bmf <- data.table::fread(file.path(DIR_RAW, "unified_bmf.csv"),
                                   select = BMF_COLS)

  # (1.2) Tigris data
  state_tigris <- tigris::states() |>
    dplyr::select("STATEFP", "STUSPS", "NAME") |>
    dplyr::rename(
      "CENSUS_STATE_FIPS" = STATEFP,
      "CENSUS_STATE_ABBR" = STUSPS,
      "CENSUS_STATE_NAME" = NAME
    )

  county_tigris <- tigris::counties() |>
    dplyr::select(STATEFP, NAMELSAD, COUNTYFP) |>
    dplyr::rename(
      "CENSUS_STATE_FIPS" = STATEFP,
      "CENSUS_COUNTY_NAME" = NAMELSAD,
      "CENSUS_COUNTY_FIPS" = COUNTYFP
    )

  cd_tigris <- tigris::congressional_districts()
  cd_transformed <- sf::st_transform(cd_tigris, 4326) |>
    dplyr::rename("CENSUS_STATE_FIPS" = STATEFP)

  county_state <- data.frame(county_tigris) |>
    tidylog::left_join(data.frame(state_tigris), by = "CENSUS_STATE_FIPS")

  district_state <- data.frame(cd_transformed) |>
    tidylog::left_join(data.frame(state_tigris), by = "CENSUS_STATE_FIPS")

  # Foreign nonprofits
  eo_xx <- data.table::fread(file.path(DIR_RAW, "foreign_nonprofits.csv")) |>
    dplyr::mutate(
      EIN2 = format_ein(EIN, to = "n"),
      EIN2 = format_ein(EIN2, to = "id")
    )
  foreign_ein <- unique(eo_xx$EIN2)

  list(
    unified_bmf    = unified_bmf,
    state_tigris   = state_tigris,
    county_tigris  = county_tigris,
    cd_transformed = cd_transformed,
    county_state   = county_state,
    district_state = district_state,
    foreign_ein    = foreign_ein
  )
}

# ==============================================================================
# (1.3) LOAD EFILE DATA
# ==============================================================================

#' Load efile CSVs for a given year
#'
#' Reads the 4 efile parts (P00 header, P01 summary, P08 revenue, P09 expenses)
#' from the year-specific raw data directory.
#'
#' @param year Integer tax year
#' @return Named list: efile_hd, efile_p01, efile_p08, efile_p09
load_efile_data <- function(year) {
  cat("== Loading efile data for year", year, "==\n")
  raw_dir <- dir_raw_year(year)

  efile_hd  <- data.table::fread(file.path(raw_dir, "efile_p00.csv"), select = EFILE_COLS)
  efile_p01 <- data.table::fread(file.path(raw_dir, "efile_p01.csv"), select = EFILE_COLS)
  efile_p08 <- data.table::fread(file.path(raw_dir, "efile_p08.csv"), select = EFILE_COLS)
  efile_p09 <- data.table::fread(file.path(raw_dir, "efile_p09.csv"), select = EFILE_COLS)

  list(
    efile_hd  = efile_hd,
    efile_p01 = efile_p01,
    efile_p08 = efile_p08,
    efile_p09 = efile_p09
  )
}

# ==============================================================================
# (2) CREATE THE SAMPLE DATASET FROM EFILE DATA
# ==============================================================================

#' Create the analysis sample from efile data
#'
#' Filters to 501(c)(3) public charities for the given tax year, excludes
#' foreign nonprofits, deduplicates group/amended returns, and merges
#' Parts VIII, IX, and I.
#'
#' @param efile_hd Header data (P00)
#' @param efile_p08 Revenue data (P08)
#' @param efile_p01 Summary data (P01)
#' @param efile_p09 Expense data (P09)
#' @param foreign_ein Character vector of foreign nonprofit EINs
#' @param year Integer tax year
#' @return Named list: efile_sample, numrec_w_gvgrnt, total_gvgrnt
create_efile_sample <- function(efile_hd, efile_p08, efile_p01, efile_p09,
                                foreign_ein, year) {
  cat("== Creating efile sample for year", year, "==\n")

  # (2.1-2.2) Only include 501c3 public charities
  eins_501c3 <- efile_hd |>
    dplyr::filter(F9_00_EXEMPT_STAT_501C3_X == "X") |>
    dplyr::pull(EIN2) |>
    unique()

  cat("  ", length(eins_501c3), " 501c3 public charities\n")

  # (2.3) Filter form 990 records: 501c3, year-specific, US-based
  efile_p08_filtered <- efile_p08 |>
    dplyr::filter(
      TAX_YEAR == as.character(year),
      RETURN_TYPE == "990",
      !EIN2 %in% foreign_ein,
      EIN2 %in% eins_501c3
    ) |>
    dplyr::mutate(
      RETURN_TIME_STAMP = lubridate::ymd_hms(RETURN_TIME_STAMP)
    )
  cat("  ", nrow(efile_p08_filtered), " records after initial filter\n")

  # (2.4) Process partial, group and amended returns
  num_partial_returns <- sum(efile_p08_filtered$RETURN_PARTIAL_X == TRUE, na.rm = TRUE)
  num_group_returns <- sum(efile_p08_filtered$RETURN_GROUP_X == TRUE, na.rm = TRUE)
  num_amended_returns <- sum(efile_p08_filtered$RETURN_AMENDED_X == TRUE, na.rm = TRUE)
  cat("  Partial:", num_partial_returns, "Group:", num_group_returns,
      "Amended:", num_amended_returns, "\n")

  efile_p08_filtered <- deduplicate_returns(efile_p08_filtered, "RETURN_GROUP_X",
                                            "RETURN_TIME_STAMP")
  efile_p08_filtered <- deduplicate_returns(efile_p08_filtered, "RETURN_AMENDED_X",
                                            "RETURN_TIME_STAMP")

  # (2.5) Quality assurance counts
  numrec_w_gvgrnt <- efile_p08_filtered |>
    dplyr::filter(!is.na(F9_08_REV_CONTR_GOVT_GRANT),
                  F9_08_REV_CONTR_GOVT_GRANT != 0) |>
    nrow()

  total_gvgrnt <- efile_p08_filtered |>
    dplyr::filter(!is.na(F9_08_REV_CONTR_GOVT_GRANT),
                  F9_08_REV_CONTR_GOVT_GRANT != 0) |>
    dplyr::summarise(total = sum(F9_08_REV_CONTR_GOVT_GRANT)) |>
    dplyr::pull(total)

  cat("  Records with govt grants:", numrec_w_gvgrnt, "\n")
  cat("  Total govt grants: $", format(total_gvgrnt, big.mark = ","), "\n")

  # (3.2) Merge Part VIII with Parts IX and I
  efile_sample <- efile_p08_filtered |>
    dplyr::select(!RETURN_TIME_STAMP) |>
    dplyr::filter(!is.na(F9_08_REV_CONTR_GOVT_GRANT),
                  F9_08_REV_CONTR_GOVT_GRANT != 0) |>
    tidylog::left_join(efile_p09, by = c("EIN2", "OBJECTID")) |>
    tidylog::left_join(efile_p01, by = c("EIN2", "OBJECTID"))

  cat("  QC: nrow(efile_sample) == numrec_w_gvgrnt:",
      nrow(efile_sample) == numrec_w_gvgrnt, "\n")
  cat("  QC: sum(govt_grant) == total_gvgrnt:",
      sum(efile_sample$F9_08_REV_CONTR_GOVT_GRANT) == total_gvgrnt, "\n")

  # Update counts to reflect actual post-join state
  numrec_w_gvgrnt <- nrow(efile_sample)
  total_gvgrnt <- sum(efile_sample$F9_08_REV_CONTR_GOVT_GRANT)

  efile_sample <- efile_sample |>
    dplyr::select(
      "EIN2",
      "F9_01_ACT_GVRN_EMPL_TOT",
      "F9_01_ACT_GVRN_VOL_TOT",
      "F9_08_REV_CONTR_GOVT_GRANT",
      "F9_08_REV_TOT_TOT",
      "F9_09_EXP_TOT_TOT",
      "F9_09_EXP_DEPREC_PROG",
      "F9_09_EXP_DEPREC_TOT",
      "F9_01_EXP_TOT_CY",
      "F9_01_REV_TOT_CY",
      "F9_01_NAFB_TOT_EOY"
    )

  list(
    efile_sample    = efile_sample,
    numrec_w_gvgrnt = numrec_w_gvgrnt,
    total_gvgrnt    = total_gvgrnt
  )
}

# ==============================================================================
# (3.1) WRANGLE BMF DATA
# ==============================================================================

#' Wrangle BMF data and map to congressional districts
#'
#' Filters to 501C3 charities, maps subsector codes, adds region lookup,
#' performs spatial join to congressional districts, and saves intermediate file.
#'
#' @param unified_bmf Raw BMF data.table
#' @param cd_transformed Congressional district sf object (WGS84)
#' @return Wrangled bmf_sample sf object
wrangle_bmf <- function(unified_bmf, cd_transformed) {
  cat("== Wrangling BMF data ==\n")

  bmf_sample <- unified_bmf |>
    dplyr::filter(NCCS_LEVEL_1 == "501C3 CHARITY") |>
    dplyr::mutate(
      SUBSECTOR = substr(NTEEV2, 1, 3),
      GEOID_TRACT_10 = substr(CENSUS_BLOCK_FIPS, 1, 11),
      CENSUS_REGION = dplyr::if_else(
        CENSUS_STATE_ABBR %in% names(CENSUS_REGION_LOOKUP),
        CENSUS_REGION_LOOKUP[CENSUS_STATE_ABBR],
        "Unmapped"
      ),
      EIN2 = format_ein(EIN2, to = "n")
    ) |>
    dplyr::mutate(
      SUBSECTOR = ifelse(SUBSECTOR == "", "UNU", SUBSECTOR),
      EIN2 = format_ein(EIN2, to = "id")
    )

  ## Map BMF coordinates to Congressional districts
  bmf_sample <- bmf_sample |>
    sf::st_as_sf(coords = c("LONGITUDE", "LATITUDE"), crs = 4326)

  bmf_sample <- sf::st_join(bmf_sample, cd_transformed, join = sf::st_intersects)

  ## Save intermediate dataset (convert geometry to WKT so fwrite can handle it)
  bmf_to_save <- data.table::as.data.table(bmf_sample)
  bmf_to_save <- bmf_to_save[, geometry := sf::st_as_text(geometry)]
  data.table::fwrite(bmf_to_save, INTERMEDIATE_BMF_SAMPLE_FILE)

  bmf_sample
}

# ==============================================================================
# (4) COMPUTE FISCAL SUSTAINABILITY METRICS
# ==============================================================================

#' Compute profit margin and at-risk indicator
#'
#' Uses profit_margin_vec() for both profit margin calculations (replacing the
#' legacy pmap_dbl approach for the first margin).
#'
#' @param efile_sample Data frame with efile financial columns
#' @return efile_sample with profit_margin, profit_margin_nogovtgrant, at_risk
compute_fiscal_metrics <- function(efile_sample) {
  cat("== Computing fiscal metrics ==\n")

  # Exploratory sorted plots (interactive use only)
  if (interactive()) {
    create_sorted_plot(efile_sample, "F9_01_REV_TOT_CY")
    create_sorted_plot(efile_sample, "F9_01_EXP_TOT_CY")
  }

  # (4.1) Profit Margin — vectorized
  efile_sample <- efile_sample |>
    dplyr::mutate(
      profit_margin = profit_margin_vec(F9_01_REV_TOT_CY, F9_01_EXP_TOT_CY)
    )

  if (interactive()) {
    create_sorted_plot(efile_sample, "profit_margin")
  }

  efile_sample <- efile_sample |>
    dplyr::mutate(
      profit_margin_nogovtgrant = profit_margin_vec(
        F9_01_REV_TOT_CY,
        F9_01_EXP_TOT_CY,
        F9_08_REV_CONTR_GOVT_GRANT
      )
    )

  if (interactive()) {
    create_sorted_plot(efile_sample, "profit_margin_nogovtgrant")
  }

  # (4.2) At Risk Indicator
  efile_sample <- efile_sample |>
    dplyr::mutate(at_risk = ifelse(profit_margin_nogovtgrant < 0, 1, 0))

  efile_sample
}

# ==============================================================================
# (5)+(6)+(7) MERGE AND POST-PROCESS
# ==============================================================================

#' Merge efile sample with BMF geography and post-process
#'
#' Performs geographic merge, imputes missing geography, computes district/
#' county names, applies factor levels, and saves intermediate + processed files.
#'
#' @param efile_sample Efile data with fiscal metrics
#' @param bmf_sample Wrangled BMF data with geography
#' @param ref_data Reference data list (from load_reference_data)
#' @param year Integer tax year
#' @param numrec_w_gvgrnt Expected row count for QA checks
#' @param total_gvgrnt Expected total govt grants for QA checks
#' @return Processed data frame (also saved to disk)
merge_and_postprocess <- function(efile_sample, bmf_sample, ref_data, year,
                                  numrec_w_gvgrnt, total_gvgrnt) {
  cat("== Merging and post-processing for year", year, "==\n")

  state_tigris     <- ref_data$state_tigris
  county_tigris    <- ref_data$county_tigris
  cd_transformed   <- ref_data$cd_transformed
  county_state     <- ref_data$county_state
  district_state   <- ref_data$district_state

  # (5) Merge with geographic data
  full_sample_int <- efile_sample |>
    tidylog::left_join(
      bmf_sample <- bmf_sample |>
        dplyr::arrange(ORG_YEAR_LAST),
      by = c("EIN2" = "EIN2"),
      multiple = "last"
    )

  cat("  QC: at_risk preserved:",
      sum(full_sample_int$at_risk) == sum(efile_sample$at_risk), "\n")
  cat("  QC: govt_grant total preserved:",
      sum(full_sample_int$F9_08_REV_CONTR_GOVT_GRANT) == total_gvgrnt, "\n")
  cat("  QC: row count preserved:",
      nrow(full_sample_int) == numrec_w_gvgrnt, "\n")

  # (6) Geographic post processing
  state_lookup <- setNames(state_tigris$CENSUS_STATE_ABBR,
                           state_tigris$CENSUS_STATE_FIPS)
  county_transformed <- sf::st_transform(county_tigris, 4326)

  # (6.1) Impute missing state information
  full_sample_int <- impute_missing_geography(
    full_sample_int, "CENSUS_STATE_ABBR", county_transformed,
    state_fips_lookup = state_lookup
  )

  # (6.2) Impute missing county information
  full_sample_int <- impute_missing_geography(
    full_sample_int, "CENSUS_COUNTY_NAME", county_transformed
  )

  # Check which counties are absent from the sample
  sample_county <- full_sample_int |>
    dplyr::select(CENSUS_STATE_ABBR, CENSUS_COUNTY_NAME) |>
    dplyr::distinct()

  absent_counties <- data.frame(county_state) |>
    dplyr::filter(!CENSUS_COUNTY_NAME %in% sample_county$CENSUS_COUNTY_NAME) |>
    dplyr::select(CENSUS_STATE_ABBR, CENSUS_COUNTY_NAME)

  dir.create(dir_intermediate_year(year), recursive = TRUE, showWarnings = FALSE)
  data.table::fwrite(absent_counties, intermediate_absent_counties_file(year))

  # (6.3) Impute missing congressional district information
  full_sample_int <- impute_missing_geography(
    full_sample_int, "CENSUS_STATE_ABBR", cd_transformed,
    state_fips_lookup = state_lookup
  )

  # Check that all districts are in the sample
  sample_district <- full_sample_int |>
    dplyr::select(CENSUS_STATE_ABBR, NAMELSAD) |>
    dplyr::distinct()

  absent_districts <- data.frame(district_state) |>
    dplyr::filter(!NAMELSAD %in% sample_district$NAMELSAD) |>
    dplyr::select(CENSUS_STATE_ABBR, NAMELSAD)

  print(absent_districts)

  # (7) Post process and save

  data.table::fwrite(full_sample_int, intermediate_full_sample_file(year))

  # Helper: format congressional district names with ordinal suffixes
  format_congress_district_names <- function(names) {
    vapply(names, function(nm) {
      m <- regmatches(nm, regexpr("\\d+", nm))
      if (length(m) == 0 || m == "") return(tolower(nm))
      n <- as.integer(m)
      paste0(make_ordinal(n), " Congressional district")
    }, character(1), USE.NAMES = FALSE)
  }

  full_sample_proc <- full_sample_int |>
    dplyr::mutate(
      SUBSECTOR = ifelse(SUBSECTOR %in% names(SUBSECTOR_CODE_MAP),
                         SUBSECTOR_CODE_MAP[SUBSECTOR],
                         "Unclassified"),
      expense_category = as.character(
        cut(F9_09_EXP_TOT_TOT,
            breaks = EXPENSE_BREAKS,
            labels = EXPENSE_LABELS,
            right = FALSE)
      ),
      expense_category = ifelse(is.na(expense_category),
                                "No Expenses Provided",
                                expense_category)
    ) |>
    dplyr::mutate(
      CENSUS_STATE_NAME = dplyr::case_when(
        CENSUS_STATE_ABBR %in% STATE_ABBREVIATIONS ~ usdata::abbr2state(CENSUS_STATE_ABBR),
        .default = "Other/unmapped jurisdictions"
      ),
      CONGRESS_DISTRICT_NAME = dplyr::case_when(
        NAMELSAD == "Congressional District" ~ "Unmapped",
      )
    ) |>
    dplyr::select(
      EIN2,
      CENSUS_REGION,
      CENSUS_COUNTY_NAME,
      CENSUS_STATE_NAME,
      NAMELSAD,
      SUBSECTOR,
      expense_category,
      F9_08_REV_CONTR_GOVT_GRANT,
      profit_margin,
      profit_margin_nogovtgrant,
      at_risk
    ) |>
    dplyr::mutate(
      CENSUS_REGION = ifelse(is.na(CENSUS_REGION), "Unmapped", CENSUS_REGION),
      NAMELSAD = ifelse(is.na(NAMELSAD), "Unmapped", NAMELSAD)
    ) |>
    dplyr::rename(
      CONGRESS_DISTRICT_NAME = NAMELSAD,
      GOVERNMENT_GRANT_DOLLAR_AMOUNT = F9_08_REV_CONTR_GOVT_GRANT,
      EXPENSE_CATEGORY = expense_category,
      PROFIT_MARGIN = profit_margin,
      PROFIT_MARGIN_NOGOVTGRANT = profit_margin_nogovtgrant,
      AT_RISK_NUM = at_risk
    ) |>
    dplyr::mutate(
      CONGRESS_DISTRICT_NAME = dplyr::case_when(
        grepl("^Congressional District \\d+$", CONGRESS_DISTRICT_NAME) ~
          format_congress_district_names(CONGRESS_DISTRICT_NAME),
        CONGRESS_DISTRICT_NAME == "Congressional District (at Large)" ~
          "Congressional district (at large)",
        CONGRESS_DISTRICT_NAME == "Delegate District (at Large)" ~
          "Delegate district (at large)",
        CONGRESS_DISTRICT_NAME == "Resident Commissioner District (at Large)" ~
          "Resident commissioner district (at large)",
        CONGRESS_DISTRICT_NAME == "Unmapped" ~ "Unmapped",
        TRUE ~ CONGRESS_DISTRICT_NAME
      )
    ) |>
    dplyr::mutate(
      CONGRESS_DISTRICT_NAME = factor(CONGRESS_DISTRICT_NAME,
                                      levels = CONGRESS_DISTRICT_LEVELS),
      SUBSECTOR = factor(SUBSECTOR, levels = SUBSECTOR_LEVELS)
    )

  ## Check Counts (informational QC — matches original pipeline behavior)
  n_proc <- nrow(full_sample_proc)
  cat("  QC: CENSUS_REGION total:", sum(table(full_sample_proc$CENSUS_REGION)) == n_proc, "\n")
  cat("  QC: CENSUS_STATE_NAME total:", sum(table(full_sample_proc$CENSUS_STATE_NAME)) == n_proc, "\n")
  cat("  QC: EXPENSE_CATEGORY total:", sum(table(full_sample_proc$EXPENSE_CATEGORY)) == n_proc, "\n")
  cat("  QC: SUBSECTOR total:", sum(table(full_sample_proc$SUBSECTOR)) == n_proc, "\n")
  cat("  QC: CONGRESS_DISTRICT_NAME total:", sum(table(full_sample_proc$CONGRESS_DISTRICT_NAME)) == n_proc, "\n")
  cat("  QC: AT_RISK_NUM preserved:", sum(full_sample_proc$AT_RISK_NUM) == sum(efile_sample$at_risk), "\n")
  cat("  QC: GOVT_GRANT total preserved:", sum(full_sample_proc$GOVERNMENT_GRANT_DOLLAR_AMOUNT) == total_gvgrnt, "\n")

  dir.create(dir_processed_year(year), recursive = TRUE, showWarnings = FALSE)
  data.table::fwrite(full_sample_proc, processed_data_file(year))

  full_sample_proc
}

# ==============================================================================
# ORCHESTRATOR
# ==============================================================================

#' Process data for a single tax year
#'
#' Composes all data processing steps. If ref_data is NULL, loads reference
#' data (BMF, TIGRIS, foreign nonprofits) from scratch. Pass pre-loaded
#' ref_data when processing multiple years to avoid redundant loads.
#'
#' @param year Integer tax year (e.g. 2021L)
#' @param ref_data Optional pre-loaded reference data list (from load_reference_data)
#' @return Processed data frame (invisibly)
process_year <- function(year, ref_data = NULL) {
  cat("\n========================================\n")
  cat("Processing year:", year, "\n")
  cat("========================================\n\n")

  # Create directories
  dir.create(DIR_RAW, recursive = TRUE, showWarnings = FALSE)
  dir.create(DIR_INTERMEDIATE, recursive = TRUE, showWarnings = FALSE)
  dir.create(dir_intermediate_year(year), recursive = TRUE, showWarnings = FALSE)
  dir.create(dir_processed_year(year), recursive = TRUE, showWarnings = FALSE)

  # Load reference data if not provided
  if (is.null(ref_data)) {
    ref_data <- load_reference_data()
  }

  # Wrangle BMF (shared, idempotent — checks if already done)
  if (!file.exists(INTERMEDIATE_BMF_SAMPLE_FILE)) {
    bmf_sample <- wrangle_bmf(ref_data$unified_bmf, ref_data$cd_transformed)
  } else {
    cat("== Loading pre-wrangled BMF ==\n")
    bmf_sample <- data.table::fread(INTERMEDIATE_BMF_SAMPLE_FILE) |>
      sf::st_as_sf(wkt = "geometry", crs = 4326)
  }

  # Load efile data for this year
  efile_data <- load_efile_data(year)

  # Create sample
  sample_result <- create_efile_sample(
    efile_hd    = efile_data$efile_hd,
    efile_p08   = efile_data$efile_p08,
    efile_p01   = efile_data$efile_p01,
    efile_p09   = efile_data$efile_p09,
    foreign_ein = ref_data$foreign_ein,
    year        = year
  )

  # Free efile raw data
  rm(efile_data)
  gc()

  # Compute metrics
  efile_sample <- compute_fiscal_metrics(sample_result$efile_sample)

  # Merge and postprocess
  full_sample_proc <- merge_and_postprocess(
    efile_sample    = efile_sample,
    bmf_sample      = bmf_sample,
    ref_data        = ref_data,
    year            = year,
    numrec_w_gvgrnt = sample_result$numrec_w_gvgrnt,
    total_gvgrnt    = sample_result$total_gvgrnt
  )

  cat("\n== Year", year, "processing complete ==\n")
  cat("  Output:", processed_data_file(year), "\n")

  invisible(full_sample_proc)
}

# ==============================================================================
# STANDALONE EXECUTION
# ==============================================================================

if (!exists(".PIPELINE_ORCHESTRATED")) {
  process_year(2021)
}
