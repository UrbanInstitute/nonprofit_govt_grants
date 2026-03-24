# ==============================================================================
# Configuration: Centralized constants, paths, mappings, and URLs
# ==============================================================================
# This file is sourced by all pipeline scripts and helpers. It contains no
# executable logic beyond defining constants. No library() calls here.

# ==============================================================================
# (1) DIRECTORY PATHS
# ==============================================================================

DIR_DATA        <- "data"
DIR_RAW         <- "data/raw"
DIR_INTERMEDIATE <- "data/intermediate"
DIR_PROCESSED   <- "data/processed"
DIR_STATE_FACTSHEETS <- "data/processed/state_factsheets"
DIR_STATE_OVERVIEWS  <- "data/processed/state_overviews"
DIR_DOCS        <- "docs"

# ==============================================================================
# (2) DATA FILES
# ==============================================================================

PROCESSED_DATA_VERSION <- "v1.0"
PROCESSED_DATA_FILE <- paste0(DIR_PROCESSED, "/full_sample_processed_",
                              PROCESSED_DATA_VERSION, ".csv")

INTERMEDIATE_FULL_SAMPLE_FILE <- paste0(DIR_INTERMEDIATE, "/full_sample.csv")
INTERMEDIATE_BMF_SAMPLE_FILE  <- paste0(DIR_INTERMEDIATE, "/bmf_sample.csv")
INTERMEDIATE_ABSENT_COUNTIES_FILE <- paste0(DIR_INTERMEDIATE, "/absent_counties.csv")

# ==============================================================================
# (2a) MULTI-YEAR SUPPORT
# ==============================================================================

SUPPORTED_YEARS <- c(2021L, 2022L, 2023L)
MIN_EXPECTED_ROWS <- 50000L

# Year-aware directory functions
dir_raw_year <- function(year) file.path(DIR_RAW, year)
dir_intermediate_year <- function(year) file.path(DIR_INTERMEDIATE, year)
dir_processed_year <- function(year) file.path(DIR_PROCESSED, year)
dir_state_factsheets_year <- function(year) file.path(DIR_PROCESSED, year, "state_factsheets")
dir_state_overviews_year <- function(year) file.path(DIR_PROCESSED, year, "state_overviews")
dir_docs_year <- function(year) file.path(DIR_DOCS, year)

# Year-aware file functions
processed_data_file <- function(year) {
  file.path(dir_processed_year(year),
            paste0("full_sample_processed_", PROCESSED_DATA_VERSION, ".csv"))
}
intermediate_full_sample_file <- function(year) {
  file.path(dir_intermediate_year(year), "full_sample.csv")
}
intermediate_absent_counties_file <- function(year) {
  file.path(dir_intermediate_year(year), "absent_counties.csv")
}
notable_grants_file <- function(year) {
  file.path(dir_intermediate_year(year), "notable_govt_grants.csv")
}

# ==============================================================================
# (3) DATA SOURCE URLs (formerly in R/data.R)
# ==============================================================================

# Legacy hardcoded URLs for backward compatibility (TY2021 only)
EFILE_URLS <- list(
  "efile_hd_2021_0225.csv" = "https://nccs-efile.s3.us-east-1.amazonaws.com/public/v2025/F9-P00-T00-HEADER-2021.csv",
  "efile_p01_2021_0225.csv" = "https://nccs-efile.s3.us-east-1.amazonaws.com/public/v2025/F9-P01-T00-SUMMARY-2021.csv",
  "efile_p08_2021_0225.csv" = "https://nccs-efile.s3.us-east-1.amazonaws.com/public/v2025/F9-P08-T00-REVENUE-2021.csv",
  "efile_p09_2021_0225.csv" = "https://nccs-efile.s3.us-east-1.amazonaws.com/public/v2025/F9-P09-T00-EXPENSES-2021.csv"
)

# Multi-year efile URL builder (v2_1 base)
EFILE_BASE_URL <- "https://nccs-efile.s3.us-east-1.amazonaws.com/public/efile_v2_1"

EFILE_PARTS <- list(
  list(part = "P00", name = "HEADER",   local = "efile_p00.csv"),
  list(part = "P01", name = "SUMMARY",  local = "efile_p01.csv"),
  list(part = "P08", name = "REVENUE",  local = "efile_p08.csv"),
  list(part = "P09", name = "EXPENSES", local = "efile_p09.csv")
)

#' Build efile download URLs for a given tax year
#' @param year Integer tax year (e.g. 2021L)
#' @return Named list: local filename -> URL
build_efile_urls <- function(year) {
  urls <- lapply(EFILE_PARTS, function(p) {
    url <- paste0(EFILE_BASE_URL, "/F9-", p$part, "-T00-", p$name, "-", year, ".CSV")
    setNames(url, p$local)
  })
  unlist(urls, recursive = FALSE)
}

BMF_URLS <- list(
  "unified_bmf.csv" = "https://nccsdata.s3.amazonaws.com/harmonized/bmf/unified/BMF_UNIFIED_V1.1.csv"
)

XX_URLS <- list(
  "foreign_nonprofits.csv" = "https://www.irs.gov/pub/irs-soi/eo_xx.csv"
)

# ==============================================================================
# (4) STATE VECTORS
# ==============================================================================

# 50 states + DC abbreviations (from usdata::state_stats$abbr)
STATE_ABBREVIATIONS <- c(
  "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL",
  "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME",
  "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
  "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
  "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
)

# 50 states + DC full names (from usdata::state_stats$state)
STATE_NAMES <- c(
  "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
  "Connecticut", "Delaware", "District of Columbia", "Florida", "Georgia",
  "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky",
  "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
  "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire",
  "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota",
  "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island",
  "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont",
  "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"
)

# ==============================================================================
# (5) COLUMN SPECIFICATIONS
# ==============================================================================

BMF_COLS <- list(
  character = c(
    "EIN2",
    "NTEEV2",
    "CENSUS_STATE_ABBR",
    "CENSUS_COUNTY_NAME",
    "ORG_YEAR_FIRST",
    "ORG_YEAR_LAST",
    "CENSUS_BLOCK_FIPS",
    "BMF_SUBSECTION_CODE",
    "NCCS_LEVEL_1"
  ),
  numeric = c("LATITUDE", "LONGITUDE")
)

EFILE_COLS <- list(
  character = c(
    "EIN2",
    "TAX_YEAR",
    "RETURN_TYPE",
    "OBJECTID",
    "URL",
    "F9_00_EXEMPT_STAT_501C3_X",
    "RETURN_TIME_STAMP",
    "F9_00_ORG_NAME_L1"
  ),
  numeric = c(
    "F9_01_ACT_GVRN_EMPL_TOT",
    "F9_01_ACT_GVRN_VOL_TOT",
    "F9_08_REV_CONTR_GOVT_GRANT",
    "F9_08_REV_TOT_TOT",
    "F9_09_EXP_TOT_TOT",
    "F9_09_EXP_DEPREC_PROG",
    "F9_01_EXP_TOT_CY",
    "F9_01_REV_TOT_CY",
    "F9_09_EXP_DEPREC_TOT",
    "F9_01_NAFB_TOT_EOY"
  ),
  logical = c(
    "RETURN_PARTIAL_X",
    "RETURN_GROUP_X",
    "RETURN_AMENDED_X"
  )
)

# ==============================================================================
# (6) CENSUS REGION MAPPING
# ==============================================================================

CENSUS_REGION_MAP <- list(
  "New England"         = c("CT", "ME", "MA", "NH", "RI", "VT"),
  "Mid-Atlantic"        = c("NJ", "NY", "PA"),
  "East North Central"  = c("IL", "IN", "MI", "OH", "WI"),
  "West North Central"  = c("IA", "KS", "MN", "MO", "NE", "ND", "SD"),
  "South Atlantic"      = c("DE", "FL", "GA", "MD", "NC", "SC", "VA", "WV", "DC"),
  "East South Central"  = c("AL", "KY", "MS", "TN"),
  "West South Central"  = c("AR", "LA", "OK", "TX"),
  "Mountain"            = c("AZ", "CO", "ID", "MT", "NV", "NM", "UT", "WY"),
  "Pacific"             = c("AK", "CA", "HI", "OR", "WA")
)

# Build reverse lookup: abbreviation -> region name
CENSUS_REGION_LOOKUP <- setNames(
  rep(names(CENSUS_REGION_MAP), lengths(CENSUS_REGION_MAP)),
  unlist(CENSUS_REGION_MAP, use.names = FALSE)
)

# ==============================================================================
# (7) EXPENSE CATEGORY DEFINITIONS
# ==============================================================================

EXPENSE_BREAKS <- c(-Inf, 100000, 500000, 1000000, 5000000, 10000000, Inf)
EXPENSE_LABELS <- c(
  "Less than $100K",
  "$100K to $499K",
  "$500K to $999K",
  "$1M to $4.9M",
  "$5M to $9.9M",
  "$10M or more"
)

# ==============================================================================
# (8) SUBSECTOR MAPPINGS
# ==============================================================================

# Code -> final display label (single-step mapping)
SUBSECTOR_CODE_MAP <- c(
  "ART" = "Arts, culture, and humanities",
  "EDU" = "Education",
  "ENV" = "Environment and animals",
  "HEL" = "Health",
  "HMS" = "Human services",
  "IFA" = "International, foreign affairs",
  "PSB" = "Public, societal benefit",
  "REL" = "Religion-related",
  "MMB" = "Mutual/membership benefit",
  "UNU" = "Unclassified",
  "UNI" = "Universities",
  "HOS" = "Hospitals"
)

SUBSECTOR_LEVELS <- c(
  "Arts, culture, and humanities",
  "Education",
  "Environment and animals",
  "Health",
  "Hospitals",
  "Human services",
  "International, foreign affairs",
  "Public, societal benefit",
  "Religion-related",
  "Mutual/membership benefit",
  "Universities",
  "Unclassified"
)

# ==============================================================================
# (9) CONGRESSIONAL DISTRICT LEVELS (generated programmatically)
# ==============================================================================

# Helper: ordinal suffix for a number
make_ordinal <- function(n) {
  if (n %% 100 %in% c(11, 12, 13)) return(paste0(n, "th"))
  suffix <- switch(as.character(n %% 10),
                   "1" = "st", "2" = "nd", "3" = "rd", "th")
  paste0(n, suffix)
}

CONGRESS_DISTRICT_LEVELS <- c(
  vapply(1:52, function(n) paste0(make_ordinal(n), " Congressional district"),
         character(1)),
  "Congressional district (at large)",
  "Delegate district (at large)",
  "Resident commissioner district (at large)",
  "Unmapped"
)

# ==============================================================================
# (10) STATE NAME FACTOR LEVELS (for national factsheet ordering)
# ==============================================================================

STATE_NAME_LEVELS <- c(
  "United States",
  STATE_NAMES,
  "Other U.S. Territories",
  "Other/unmapped jurisdictions"
)
