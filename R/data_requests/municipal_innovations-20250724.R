#------------------------------------------------------------------------------
# File: municipal_innovations-20250724.R
# Programmer: Thiyaghessan [tpoongundranar@urban.org]
# Date Created: 2025-07-24
# Date Last Edited: 2025-07-25
#
# Purpose: The file does the data engineering from the financial risk analysis
# tool for 5 policy areas across 19 cities for use in Municipal Innovation 
# Project
#
# Dependencies
#   - tigris
#   - sf
#   - dplyr
#   - purrr
#   - readr
#   - here
#   - writexl
#   - R/data_requests/municipal_innovations-20250724.R # Helper functions
#
# Datasets
#  - BMF Data: https://nccsdata.s3.amazonaws.com/harmonized/bmf/unified/BMF_UNIFIED_V1.1.csv
#  - NTEE Code Data: data/data_requests/municipal_innovations/ntee_codes.csv
#  - Financial Metric Data: data/processed/full_sample_processed_v1.0.csv
#
# Notes
# (1) - Prepare spatial data for 20 cities
# (2) - Load BMF data, NTEE Code Data and Financial Metric Data
# (3) - Transform and merge data
# (4) - Load into indivual tables for saving in an excel workbook for review.
#-------------------------------------------------------------------------------

# Libraries
library(tigris)
library(sf)
library(dplyr)
library(here)
library(purrr)

# Helper Scripts
source(here::here("R", "data_requests/municipal_innovations_utils-20250724.R"))

# (1) - Prepare spatial data for 20 cities

city_boundary_params <- list(
  "Philadelphia" = list(geo = "county", state = "PA", fips = "42101"), # Philadelphia, PA (City and County are coterminous: Philadelphia County)
  "Memphis" = list(geo = "place", state = "TN", name = "Memphis"), # Memphis is an incorporated place within Shelby County
  "New Orleans" = list(geo = "county", state = "LA", fips = "22071"), # New Orleans, LA (City and Parish (County) are coterminous: Orleans Parish)
  "Tulsa" = list(geo = "place", state = "OK", name = "Tulsa"),
  "Los Angeles" = list(geo = "place", state = "CA", name = "Los Angeles"), # Los Angeles, CA (Los Angeles County)
  "Detroit" = list(geo = "place", state = "MI", name = "Detroit"),
  "Louisville" = list(geo = "county", state = "KY", fips = "21111"), # Louisville/Jefferson County Metro, KY (consolidated city-county: Jefferson County)
  "New York" = list(geo = "county", state = "NY", fips = c("36005", "36047", "36061", "36081", "36085"), name = "New York City"), # New York, NY (The five counties/boroughs)
  "Atlanta" = list(geo = "place", state = "GA", name = "Atlanta"),
  "Kansas City" = list(geo = "place", state = "MO", name = "Kansas City"),
  "Boston" = list(geo = "place", state = "MA", name = "Boston"),
  "Nashville" = list(geo = "county", state = "TN", fips = "47037"), # Nashville, TN (Nashville-Davidson -- consolidated city-county: Davidson County)
  "Cleveland" = list(geo = "place", state = "OH", name = "Cleveland"),
  "Oklahoma City" = list(geo = "place", state = "OK", name = "Oklahoma City"),
  "Portland" = list(geo = "place", state = "OR", name = "Portland"),
  "San Diego" = list(geo = "place", state = "CA", name = "San Diego"),
  "Denver" = list(geo = "county", state = "CO", fips = "08031"), # Denver, CO (City and County are coterminous: Denver County)
  "San Francisco" = list(geo = "county", state = "CA", fips = "06075"), # San Francisco, CA (City and County are coterminous: San Francisco County)
  "Baltimore" = list(geo = "county", state = "MD", fips = "24510") # Baltimore City is a county-equivalent
)

city_boundaries <- purrr::map(city_boundary_params, get_city_geos)

# Perform a Union of all NYC Buroughs
city_boundaries[["New York"]] <- sf::st_union(city_boundaries[["New York"]]) |>
  data.frame() |>
  sf::st_as_sf() |>
  dplyr::mutate(NAME = "New York City", NAMELSAD = "New York City")

city_boundaries_df <- purrr::list_rbind(city_boundaries) |>
  sf::st_as_sf() |>
  dplyr::select(NAME) |>
  dplyr::rename(CITY = NAME)

# Save outputs
readr::write_csv(city_boundaries_df, "data/data_requests/municipal_innovations/tigris_city_boundaries.csv")

# (2) - Load  BMF data, NTEE Code Data and Financial Metric Data

# (2.1) - Financial Metric Data from Financial Risk Tool
nonprofit_financial_metrics <- data.table::fread("data/processed/full_sample_processed_v1.0.csv") |>
  dplyr::select(
    EIN2,
    SUBSECTOR,
    CENSUS_STATE_NAME,
    CENSUS_COUNTY_NAME,
    CONGRESS_DISTRICT_NAME,
    GOVERNMENT_GRANT_DOLLAR_AMOUNT,
    PROFIT_MARGIN,
    PROFIT_MARGIN_NOGOVTGRANT,
    AT_RISK_NUM
  )

# (2.2) - BMF Data for spatial join
if (!file.exists("data/raw/unified_bmf.csv")) {
  download.file(
    "https://nccsdata.s3.amazonaws.com/harmonized/bmf/unified/BMF_UNIFIED_V1.1.csv",
    destfile = "data/raw/unified_bmf.csv"
  )
}
bmf_sample <- data.table::fread("data/raw/unified_bmf.csv") |>
  dplyr::filter(EIN2 %in% nonprofit_financial_metrics$EIN2) |>
  dplyr::group_by(EIN2) |>
  dplyr::arrange(ORG_YEAR_LAST) |>
  dplyr::slice_max(ORG_YEAR_LAST)

bmf_sf <- bmf_sample |>
  sf::st_as_sf(coords = c("LONGITUDE", "LATITUDE"), crs = 4269) |>
  sf::st_make_valid()

bmf_city_boundaries <- sf::st_join(bmf_sf, city_boundaries_df, join = sf::st_within)

readr::write_csv(bmf_city_boundaries, "data/data_requests/municipal_innovations/bmf_city_mappings.csv")

# (2.3) - NTEE Code Data
policy_ntee_map <- readr::read_csv("data/data_requests/municipal_innovations/ntee_codes.csv")
# Convert NTEEV2 format for merging with BMF and select relevant policy areas
policy_areas <- policy_ntee_map |>
  dplyr::select(NTEEV2, `Policy Area`) |>
  dplyr::mutate(NTEEV2 = sapply(NTEEV2, convert_format))
setequal(unique(policy_areas$`Policy Area`), c("Education", "Medicaid", "SNAP", "Housing", "Immigration", "Unmapped"))

# (2.3) - Transform and merge data

# Only keep BMF records belonging to a City
bmf_muni_sample <- bmf_city_boundaries |>
  dplyr::mutate(CITY = ifelse(is.na(CITY), "Other Cities", CITY))
length(unique(bmf_muni_sample$CITY)) == 20
# Merge data together for combined cities metrics
cities_metrics <- bmf_muni_sample |>
  dplyr::select(EIN2, CITY, NTEEV2) |>
  tidylog::left_join(nonprofit_financial_metrics) |>
  tidylog::left_join(policy_areas) |>
  dplyr::mutate(`Policy Area` = ifelse(is.na(`Policy Area`), "Unmapped", `Policy Area`))
# Don't filter out NA since we need them for the overall city counts
setequal(unique(cities_metrics$`Policy Area`), c("Education", "Medicaid", "SNAP", "Housing", "Immigration", "Unmapped"))

# (3) - Summarize Data

# usa level data
usa_summaries <- summarize_city_metrics(nonprofit_financial_metrics, grouping_cols = NULL) |>
  sf::st_drop_geometry()

# Get state level summaries
state_summaries <- summarize_city_metrics(nonprofit_financial_metrics, grouping_cols = "CENSUS_STATE_NAME") |>
  dplyr::filter(CENSUS_STATE_NAME %in% cities_metrics$CENSUS_STATE_NAME)

# City level summaries
city_summaries <- summarize_city_metrics(cities_metrics, grouping_cols = c("CENSUS_STATE_NAME", "CITY")) |>
  sf::st_drop_geometry()

# City Policy Summaries
city_policy_summaries <- summarize_city_metrics(cities_metrics, grouping_cols = c("CENSUS_STATE_NAME", "CITY", "Policy Area")) |>
  sf::st_drop_geometry()

# Policy - Summaries
policy_summaries <- summarize_city_metrics(cities_metrics, grouping_cols = c("Policy Area")) |>
  sf::st_drop_geometry()
  
# (4) Load into individual tables

writexl::write_xlsx(
  list(
    "usa-summaries" = usa_summaries,
    "state-level-summaries" = state_summaries,
    "city-summaries" = city_summaries,
    "city-policy-area-summaries" = city_policy_summaries,
    "policy-area-summaries" = policy_summaries
  ),
  path = "data/data_requests/municipal_innovations/factsheet_metrics-20250725.xlsx"
)
