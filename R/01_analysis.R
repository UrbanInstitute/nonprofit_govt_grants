# Script Header
# Title: Federal Funding Freeze Blog Post
# Date created: 2025-02-03
# Date last modified: 2025-02-06
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