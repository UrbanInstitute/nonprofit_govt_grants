# Script Header
# Title: Federal Funding Freeze Blog Post
# Date created: 2025-02-03
# Date last modified: 2025-02-13
# Description: This script contains code to analyze data for HTML fact sheets 
# on nonprofits's fiscal sustainability and reliance on government grants for 
# Tax Year 2021. It creates data.frames for fact sheets for each disaggregation.

# Packages
library(tidyverse)
library(data.table)
library(purrr)
library(tidyr)

# Load in data
full_sample_proc <- data.table::fread("data/intermediate/full_sample_processed.csv")

# Helper scripts
source("R/summarise_data.R")

# Pull Massachusetts data for testing

# Massachussets by County

ma_county <- full_sample_proc |>
  dplyr::filter(CENSUS_STATE_ABBR == "MA") |>
  dplyr::group_by(CENSUS_COUNTY_NAME) |>
  dplyr::summarise(
    number_with_govt_grants = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT > 0, na.rm = TRUE),
    total_govt_grants = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT, na.rm = TRUE),
    median_profit_margin = median(PROFIT_MARGIN, na.rm = TRUE),
    median_profit_margin_no_govt_grants = median(PROFIT_MARGIN_NOGOVTGRANT, na.rm = TRUE),
    median_days_cash_on_hand = median(DAYS_CASH_ON_HAND, na.rm = TRUE),
    lessthan30days_cash_on_hand = sum(DAYS_CASH_ON_HAND < 30, na.rm = TRUE),
    btwn30and90days_cash_on_hand = sum(DAYS_CASH_ON_HAND >= 30 & DAYS_CASH_ON_HAND < 90, na.rm = TRUE),
    morethan90days_cash_on_hand = sum(DAYS_CASH_ON_HAND >= 90, na.rm = TRUE),
    median_months_cash_on_hand = median(MONTHS_CASH_ON_HAND, na.rm = TRUE),
    lessthan1month_cash_on_hand = sum(MONTHS_CASH_ON_HAND < 1, na.rm = TRUE),
    btwn1and3months_cash_on_hand = sum(MONTHS_CASH_ON_HAND >= 1 & MONTHS_CASH_ON_HAND < 3, na.rm = TRUE),
    morethan3months_cash_on_hand = sum(MONTHS_CASH_ON_HAND >= 3, na.rm = TRUE)
  )

numnonprofits_county <- bmf_sample |>
  dplyr::filter(CENSUS_STATE_ABBR == "MA",
               ORG_YEAR_LAST >= 2021) |>
  dplyr::group_by(CENSUS_COUNTY_NAME) |>
  dplyr::summarise(num_nonprofits = dplyr::n_distinct(EIN2)) |>
  sf::st_drop_geometry()

ma_county <- tidylog::left_join(ma_county, numnonprofits_county, by = "CENSUS_COUNTY_NAME")

#  Create dataset for testing
factsheet_df <- summarise_data(full_sample_proc, "CENSUS_STATE_ABBR", "CA", "EXPENSE_CATEGORY")
data.table::fwrite(factsheet_df, "data/intermediate/factsheet_test_expense_df.csv")
factsheet_df <- summarise_data(full_sample_proc, "CENSUS_STATE_ABBR", "CA", "SUBSECTOR")
data.table::fwrite(factsheet_df, "data/intermediate/factsheet_test_subsector_df.csv")

# Groupings
groupings <- c("EXPENSE_CATEGORY", "SUBSECTOR")

# (1) - Create national level datasets
for (grouping in c("EXPENSE_CATEGORY", "SUBSECTOR")) {
  factsheet_df_national <- summarise_data(full_sample_proc, "CENSUS_STATE_ABBR", "US", grouping, national = TRUE)
  data.table::fwrite(factsheet_df_national, paste0("data/processed/national_", tolower(grouping), ".csv"))
}

# (2) - Create regional datasets
regions <- c("West South Central", "New England", "South Atlantic", "West North Central", 
             "Mid-Atlantic", "East North Central", "Pacific", "Mountain", 
             "East South Central")
cross_df <- tidyr::crossing(group = groupings, region = regions)
cross_df %>%
  purrr::pwalk(function(group, region) {
    factsheet_df_regional <- summarise_data(full_sample_proc, "CENSUS_REGION", region, group)
    data.table::fwrite(
      factsheet_df_regional,
      paste0("data/processed/regional_", tolower(group), "_", tolower(region), ".csv")
    )
  })

# (3) - Create state level datasets
states <- unique(usdata::state_stats$abbr)
cross_df <- tidyr::crossing(group = groupings, state = states)
cross_df |> 
  purrr::pwalk(function(group, state) {
    factsheet_df_state <- summarise_data(full_sample_proc, "CENSUS_STATE_ABBR", state, group)
    data.table::fwrite(
      factsheet_df_state,
      paste0("data/processed/state_", tolower(group), "_", tolower(state), ".csv")
    )
  })

# TODO
# Create tables for congressional districts