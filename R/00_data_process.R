# Script Header
# Title: Federal Funding Freeze Blog Post
# Date created: 2025-01-31
# Date last modified: 2025-07-16
# Description: This script contains code to wrangle and process data for HTML
# fact sheets on nonprofits's fiscal sustainability and reliance on government
# grants for Tax Year 2021. It also adds employment data requested by Candid on
# 28th 2025.
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

# Create directories (in case 00_download.R was not run)
dir.create(DIR_RAW, recursive = TRUE, showWarnings = FALSE)
dir.create(DIR_INTERMEDIATE, recursive = TRUE, showWarnings = FALSE)
dir.create(DIR_PROCESSED, recursive = TRUE, showWarnings = FALSE)

# ==============================================================================
# (1) LOAD RAW DATA
# ==============================================================================

# (1.1) BMF Data

unified_bmf <- data.table::fread(file.path(DIR_RAW, "unified_bmf.csv"),
                                 select = BMF_COLS)

# (1.2) Tigris data — ensure sample is complete with all state/county/district
#        combinations

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

# (1.3) Efile data

efile_21_hd_raw  <- data.table::fread(file.path(DIR_RAW, "efile_hd_2021_0225.csv"),  select = EFILE_COLS)
efile_21_p01_raw <- data.table::fread(file.path(DIR_RAW, "efile_p01_2021_0225.csv"), select = EFILE_COLS)
efile_21_p05_raw <- data.table::fread(file.path(DIR_RAW, "efile_p05_2021_0225.csv"), select = EFILE_COLS)
efile_21_p08_raw <- data.table::fread(file.path(DIR_RAW, "efile_p08_2021_0225.csv"), select = EFILE_COLS)
efile_21_p09_raw <- data.table::fread(file.path(DIR_RAW, "efile_p09_2021_0225.csv"), select = EFILE_COLS)
efile_21_p10_raw <- data.table::fread(file.path(DIR_RAW, "efile_p10_2021_0225.csv"), select = EFILE_COLS)

# ==============================================================================
# (2) CREATE THE SAMPLE DATASET FROM EFILE DATA
# ==============================================================================

# (2.1) Exclude foreign nonprofits

eo_xx <- data.table::fread(file.path(DIR_RAW, "foreign_nonprofits.csv")) |>
  dplyr::mutate(
    EIN2 = format_ein(EIN, to = "n"),
    EIN2 = format_ein(EIN2, to = "id")
  )

length(intersect(efile_21_p08_raw$EIN2, eo_xx$EIN2))
### 537 EINs belong to foreign nonprofits

foreign_ein <- unique(eo_xx$EIN2)

# (2.2) Only include 501c3 public charities

eins_501c3 <- efile_21_hd_raw |>
  dplyr::filter(F9_00_EXEMPT_STAT_501C3_X == "X") |>
  dplyr::pull(EIN2) |>
  unique()

length(eins_501c3)
# 392,704 501c3 public charities

# (2.3) Filter form 990 records: 501c3 public charities, TY2021, US-based

efile_21_p08 <- efile_21_p08_raw |>
  dplyr::filter(
    TAX_YEAR == "2021",
    RETURN_TYPE == "990",
    !EIN2 %in% foreign_ein,
    EIN2 %in% eins_501c3
  ) |>
  dplyr::mutate(
    RETURN_TIME_STAMP = lubridate::ymd_hms(RETURN_TIME_STAMP)
  )
### 246,020 records

# (2.4) Process partial, group and amended returns

num_partial_returns <- sum(efile_21_p08$RETURN_PARTIAL_X == TRUE, na.rm = TRUE)
### 2,085 partial returns — kept as-is

num_group_returns <- sum(efile_21_p08$RETURN_GROUP_X == TRUE, na.rm = TRUE)
### 309 group returns

num_amended_returns <- sum(efile_21_p08$RETURN_AMENDED_X == TRUE, na.rm = TRUE)
### 3,977 amended returns

# Deduplicate group returns (keep most recent per EIN)
efile_21_p08 <- deduplicate_returns(efile_21_p08, "RETURN_GROUP_X", "RETURN_TIME_STAMP")
### 246,014 records. 4 duplicates discarded.

# Deduplicate amended returns (keep most recent per EIN)
efile_21_p08 <- deduplicate_returns(efile_21_p08, "RETURN_AMENDED_X", "RETURN_TIME_STAMP")
### 242,505 records. 3,511 duplicates discarded.

# (2.5) Quality assurance counts

numrec_w_part08 <- nrow(efile_21_p08)
### 242,503

numrec_w_gvgrnt <- efile_21_p08 |>
  dplyr::filter(!is.na(F9_08_REV_CONTR_GOVT_GRANT),
                F9_08_REV_CONTR_GOVT_GRANT != 0) |>
  nrow()
### 103,478 records

total_gvgrnt <- efile_21_p08 |>
  dplyr::filter(!is.na(F9_08_REV_CONTR_GOVT_GRANT),
                F9_08_REV_CONTR_GOVT_GRANT != 0) |>
  dplyr::summarise(total = sum(F9_08_REV_CONTR_GOVT_GRANT)) |>
  dplyr::pull(total)
### $267,741,882,036

# ==============================================================================
# (3) WRANGLE DATA
# ==============================================================================

# (3.1) Wrangle BMF Data

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

## QC Check
nrow(bmf_sample) == nrow(unified_bmf[unified_bmf$NCCS_LEVEL_1 == "501C3 CHARITY"])

## Map BMF coordinates to Congressional districts
bmf_sample <- bmf_sample |>
  sf::st_as_sf(coords = c("LONGITUDE", "LATITUDE"), crs = 4326)

bmf_sample <- sf::st_join(bmf_sample, cd_transformed, join = sf::st_intersects)

## Save intermediate dataset
data.table::fwrite(bmf_sample, INTERMEDIATE_BMF_SAMPLE_FILE)

rm(unified_bmf)
gc()

# (3.2) Wrangle efile data — merge Part VIII with Parts IX, X, and I

efile_sample <- efile_21_p08 |>
  dplyr::select(!RETURN_TIME_STAMP) |>
  dplyr::filter(!is.na(F9_08_REV_CONTR_GOVT_GRANT),
                F9_08_REV_CONTR_GOVT_GRANT != 0) |>
  tidylog::left_join(efile_21_p09_raw, by = c("EIN2", "OBJECTID")) |>
  tidylog::left_join(efile_21_p10_raw, by = c("EIN2", "OBJECTID")) |>
  tidylog::left_join(efile_21_p01_raw, by = c("EIN2", "OBJECTID"))

nrow(efile_sample) == numrec_w_gvgrnt
sum(efile_sample$F9_08_REV_CONTR_GOVT_GRANT) == total_gvgrnt

rm(efile_21_p08_raw, efile_21_p09_raw, efile_21_p10_raw,
   efile_21_p01_raw, efile_21_p05_raw, efile_21_hd_raw, efile_21_p08)
gc()

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
    "F9_10_ASSET_CASH_EOY",
    "F9_10_ASSET_SAVING_EOY",
    "F9_10_ASSET_PLEDGE_NET_EOY",
    "F9_10_ASSET_ACC_NET_EOY",
    "F9_10_NAFB_UNRESTRICT_EOY",
    "F9_10_ASSET_LAND_BLDG_NET_EOY",
    "F9_10_LIAB_TAX_EXEMPT_BOND_EOY",
    "F9_10_LIAB_MTG_NOTE_EOY",
    "F9_10_LIAB_NOTE_UNSEC_EOY",
    "F9_01_EXP_TOT_CY",
    "F9_01_REV_TOT_CY",
    "F9_01_NAFB_TOT_EOY"
  )

nrow(efile_sample) == numrec_w_gvgrnt
sum(efile_sample$F9_08_REV_CONTR_GOVT_GRANT) == total_gvgrnt

# ==============================================================================
# (4) COMPUTE FISCAL SUSTAINABILITY METRICS
# ==============================================================================

# (4.1) Profit Margin — with and without government grants

# Exploratory sorted plots (interactive use only)
if (interactive()) {
  create_sorted_plot(efile_sample, "F9_01_REV_TOT_CY")
  create_sorted_plot(efile_sample, "F9_01_EXP_TOT_CY")
}

efile_sample <- efile_sample |>
  dplyr::mutate(profit_margin = purrr::pmap_dbl(
    list(F9_01_REV_TOT_CY, F9_01_EXP_TOT_CY),
    profit_margin,
    .progress = TRUE
  ))

summary(efile_sample$profit_margin)

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

summary(efile_sample$profit_margin_nogovtgrant)

if (interactive()) {
  create_sorted_plot(efile_sample, "profit_margin_nogovtgrant")
}

# (4.2) At Risk Indicator — negative profit margin without govt grants

efile_sample <- efile_sample |>
  dplyr::mutate(at_risk = ifelse(profit_margin_nogovtgrant < 0, 1, 0))

summary(efile_sample$at_risk)
table(efile_sample$at_risk)
### 33,788 not at risk, 69,687 at risk

# ==============================================================================
# (5) MERGE WITH GEOGRAPHIC DATA FROM THE UNIFIED BMF
# ==============================================================================

full_sample_int <- efile_sample |>
  tidylog::left_join(
    bmf_sample <- bmf_sample |>
      dplyr::arrange(ORG_YEAR_LAST),
    by = c("EIN2" = "EIN2"),
    multiple = "last"
  )

sum(full_sample_int$at_risk) == sum(efile_sample$at_risk)
sum(full_sample_int$F9_08_REV_CONTR_GOVT_GRANT) == total_gvgrnt
nrow(full_sample_int) == numrec_w_gvgrnt

# ==============================================================================
# (6) GEOGRAPHIC POST PROCESSING
# ==============================================================================

# State FIPS to abbreviation lookup (used by imputation helper)
state_lookup <- setNames(state_tigris$CENSUS_STATE_ABBR,
                         state_tigris$CENSUS_STATE_FIPS)

# Transform counties to WGS84 for spatial joins
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

data.table::fwrite(absent_counties, INTERMEDIATE_ABSENT_COUNTIES_FILE)
### 180 counties are not in the sample

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
### All districts are in the sample but not all counties

# ==============================================================================
# (7) POST PROCESS AND SAVE
# ==============================================================================

data.table::fwrite(full_sample_int, INTERMEDIATE_FULL_SAMPLE_FILE)

# Helper: format congressional district names with ordinal suffixes
format_congress_district_names <- function(names) {
  vapply(names, function(nm) {
    # Match "Congressional District N"
    m <- regmatches(nm, regexpr("\\d+", nm))
    if (length(m) == 0 || m == "") return(tolower(nm))
    n <- as.integer(m)
    paste0(make_ordinal(n), " Congressional district")
  }, character(1), USE.NAMES = FALSE)
}

full_sample_proc <- full_sample_int |>
  dplyr::mutate(
    # Map subsector codes to labels in one step
    SUBSECTOR = ifelse(SUBSECTOR %in% names(SUBSECTOR_CODE_MAP),
                       SUBSECTOR_CODE_MAP[SUBSECTOR],
                       "Unclassified"),
    # Categorize expenses using cut()
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
    # Format congressional district names with ordinal suffixes
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

## Check Counts
sum(table(full_sample_proc$CENSUS_REGION)) == numrec_w_gvgrnt
sum(table(full_sample_proc$CENSUS_STATE_NAME)) == numrec_w_gvgrnt
sum(table(full_sample_proc$EXPENSE_CATEGORY)) == numrec_w_gvgrnt
sum(table(full_sample_proc$SUBSECTOR)) == numrec_w_gvgrnt
sum(table(full_sample_proc$CONGRESS_DISTRICT_NAME)) == numrec_w_gvgrnt
sum(full_sample_proc$AT_RISK_NUM) == sum(efile_sample$at_risk)
sum(full_sample_proc$GOVERNMENT_GRANT_DOLLAR_AMOUNT) == total_gvgrnt

data.table::fwrite(full_sample_proc, PROCESSED_DATA_FILE)
