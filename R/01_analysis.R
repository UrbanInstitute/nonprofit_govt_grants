# Script Header
# Title: Federal Funding Freeze Blog Post
# Date created: 2025-02-03
# Date last modified: 2025-02-18
# Description: This script contains code to analyze data for HTML fact sheets 
# on nonprofits's fiscal sustainability and reliance on government grants for 
# Tax Year 2021. It creates data.frames for fact sheets for each disaggregation.

# Packages
library(tidyverse)
library(data.table)
library(purrr)
library(tidyr)
library(rlang)
library(janitor)
library(writexl)

# Create directory
dir.create("data/processed/state_factsheets", showWarnings = FALSE)
dir.create("data/processed/state_overviews", showWarnings = FALSE)

# Scripts
source("R/summarise_data.R")

# Load in data
full_sample_proc <- data.table::fread("data/intermediate/full_sample_processed.csv")
absent_counties <- data.table::fread("data/intermediate/absent_counties.csv")

# (1) - National Summaries
national <- summarize_nonprofit_data(full_sample_proc)

national_bystate <- summarize_nonprofit_data(full_sample_proc,
                                             group_var = "CENSUS_STATE_NAME",
                                             group_var_rename = "State") |>
  dplyr::bind_rows(dplyr::mutate(national, State = "Total"))
national_bysize <- summarize_nonprofit_data(full_sample_proc,
                                           group_var = "EXPENSE_CATEGORY",
                                           group_var_rename = "Size") |>
  dplyr::bind_rows(dplyr::mutate(national, Size = "Total"))

national_bysubsector <- summarize_nonprofit_data(full_sample_proc,
                                                 group_var = "SUBSECTOR",
                                                 group_var_rename = "Subsector") |> dplyr::bind_rows(dplyr::mutate(national, Subsector = "Total"))


datasets <- list(
  "national_bystate" = national_bystate,
  "national_bysize" = national_bysize,
  "national_bysubsector" = national_bysubsector
)
# Save datasets to the processed/ folder
purrr::walk2(
  datasets,
  names(datasets),
  function(df, name) {
    data.table::fwrite(df, paste0("data/processed/", name, ".csv"))
  }
)
# Create an .xlsx spreadsheet for the team to view
writexl::write_xlsx(list(
  "National" = national,
  "State" = national_bystate,
  "Size" = national_bysize,
  "Subsector" = national_bysubsector
), path = "data/processed/national_combined.xlsx")

# (2) - State level summary
states <- as.character(usdata::state_stats$state)

for (state in states) {
  state_sample <- full_sample_proc |>
    dplyr::filter(CENSUS_STATE_NAME == state)
  
  state_overall <- summarize_nonprofit_data(state_sample)
  
  state_bycounty <- summarize_nonprofit_data(state_sample,
                                             group_var = "CENSUS_COUNTY_NAME",
                                             group_var_rename = "County") |>
    dplyr::bind_rows(dplyr::mutate(state_overall, County = "Total"))
  
  state_bydistrict <- summarize_nonprofit_data(state_sample,
                                               group_var = "CONGRESS_DISTRICT_NAME",
                                               group_var_rename = "Congressional District") |>
    dplyr::bind_rows(dplyr::mutate(state_overall, `Congressional District` = "Total"))
  
  state_bysize <- summarize_nonprofit_data(state_sample,
                                           group_var = "EXPENSE_CATEGORY",
                                           group_var_rename = "Size") |>
    dplyr::bind_rows(dplyr::mutate(state_overall, Size = "Total"))
  
  state_bysubsector <- summarize_nonprofit_data(state_sample,
                                                group_var = "SUBSECTOR",
                                                group_var_rename = "Subsector") |>
    dplyr::bind_rows(dplyr::mutate(state_overall, Subsector = "Total"))
  
  datasets <- list(
    "_bycounty" = state_bycounty,
    "_bydistrict" = state_bydistrict,
    "_bysize" = state_bysize,
    "_bysubsector" = state_bysubsector
  )
  
  # Save datasets to the processed/ folder
  purrr::walk2(datasets, names(datasets), function(df, name) {
    data.table::fwrite(df, paste0("data/processed/state_factsheets/", 
                                  gsub(" ", "-", tolower(state)), 
                                  name, 
                                  ".csv"))
  })
  
  writexl::write_xlsx(
    list(
      "Overall" = state_overall,
      "County" = state_bycounty,
      "Congressional District" = state_bydistrict,
      "Size" = state_bysize,
      "Subsector" = state_bysubsector
    ),
    path = paste0("data/processed/state_overviews/", 
                  gsub(" ", "-", tolower(state)), 
                  "_overview.xlsx")
  )
  
}

# (3) - QC Dataset

qc <- full_sample_proc |>
  dplyr::mutate(
    state_name = CENSUS_STATE_NAME
  )

qc_state <- summarize_nonprofit_data(qc,
                                     group_var = "state_name",
                                     group_var_rename = "state_name",
                                     qc = TRUE) |>
  janitor::adorn_totals("row", c("No. of 990 Filers w/ Gov Grants",
                        "Total Gov Grants ($)")) |>
  dplyr::mutate(
    `No. of 990 Filers w/ Gov Grants` = scales::comma(`No. of 990 Filers w/ Gov Grants`),
    `Total Gov Grants ($)` = scales::dollar(`Total Gov Grants ($)`),
    `Operating Surplus (%)` = scales::percent(`Operating Surplus (%)`, accuracy = 0.01),
    `Operating Surplus w/o Gov Grants (%)` = scales::percent(`Operating Surplus w/o Gov Grants (%)`, accuracy = 0.01),
    `Share of 990 Filers w/ Gov Grants at Risk` = scales::percent(`Share of 990 Filers w/ Gov Grants at Risk`, accuracy = 0.01)
  )

qc_district <- summarize_nonprofit_data(full_sample_proc,
                                              group_var = "CONGRESS_DISTRICT_NAME",
                                              group_var_rename = "Congressional District",
                                              qc = TRUE)  |>
  janitor::adorn_totals("row", c("No. of 990 Filers w/ Gov Grants",
                                 "Total Gov Grants ($)")) |>
  dplyr::mutate(
    `No. of 990 Filers w/ Gov Grants` = scales::comma(`No. of 990 Filers w/ Gov Grants`),
    `Total Gov Grants ($)` = scales::dollar(`Total Gov Grants ($)`),
    `Operating Surplus (%)` = scales::percent(`Operating Surplus (%)`, accuracy = 0.01),
    `Operating Surplus w/o Gov Grants (%)` = scales::percent(`Operating Surplus w/o Gov Grants (%)`, accuracy = 0.01),
    `Share of 990 Filers w/ Gov Grants at Risk` = scales::percent(`Share of 990 Filers w/ Gov Grants at Risk`, accuracy = 0.01)
  )

# County
absent_counties <- absent_counties |>
  dplyr::mutate(
    CENSUS_STATE_NAME = usdata::abbr2state(CENSUS_STATE_ABBR)
  ) |>
  dplyr::mutate(
    CENSUS_STATE_NAME = ifelse(is.na(CENSUS_STATE_NAME), 
                                "Other U.S. Territories", 
                                CENSUS_STATE_NAME)
  ) |>
  dplyr::select(! CENSUS_STATE_ABBR) |>
  dplyr::rename(County = CENSUS_COUNTY_NAME)
  

qc_county <- summarize_nonprofit_data(full_sample_proc,
                                            group_var = "CENSUS_COUNTY_NAME",
                                            group_var_rename = "County",
                                            qc = TRUE)  |>
  dplyr::bind_rows(absent_counties) |>
  janitor::adorn_totals("row", c("No. of 990 Filers w/ Gov Grants",
                                 "Total Gov Grants ($)")) |>
  dplyr::mutate(
    `No. of 990 Filers w/ Gov Grants` = scales::comma(`No. of 990 Filers w/ Gov Grants`),
    `Total Gov Grants ($)` = scales::dollar(`Total Gov Grants ($)`),
    `Operating Surplus (%)` = scales::percent(`Operating Surplus (%)`, accuracy = 0.01),
    `Operating Surplus w/o Gov Grants (%)` = scales::percent(`Operating Surplus w/o Gov Grants (%)`, accuracy = 0.01),
    `Share of 990 Filers w/ Gov Grants at Risk` = scales::percent(`Share of 990 Filers w/ Gov Grants at Risk`, accuracy = 0.01)
  )

qc_county[is.na(qc_county)] <- "0"

qc_size <- summarize_nonprofit_data(full_sample_proc,
                                          group_var = "EXPENSE_CATEGORY",
                                          group_var_rename = "Size",
                                          qc = TRUE)  |>
  janitor::adorn_totals("row", c("No. of 990 Filers w/ Gov Grants",
                                 "Total Gov Grants ($)")) |>
  dplyr::mutate(
    `No. of 990 Filers w/ Gov Grants` = scales::comma(`No. of 990 Filers w/ Gov Grants`),
    `Total Gov Grants ($)` = scales::dollar(`Total Gov Grants ($)`),
    `Operating Surplus (%)` = scales::percent(`Operating Surplus (%)`, accuracy = 0.01),
    `Operating Surplus w/o Gov Grants (%)` = scales::percent(`Operating Surplus w/o Gov Grants (%)`, accuracy = 0.01),
    `Share of 990 Filers w/ Gov Grants at Risk` = scales::percent(`Share of 990 Filers w/ Gov Grants at Risk`, accuracy = 0.01)
  )

qc_subsector <- summarize_nonprofit_data(full_sample_proc,
                                               group_var = "SUBSECTOR",
                                               group_var_rename = "Subsector",
                                               qc = TRUE)  |>
  janitor::adorn_totals("row", c("No. of 990 Filers w/ Gov Grants",
                                 "Total Gov Grants ($)")) |>
  dplyr::mutate(
    `No. of 990 Filers w/ Gov Grants` = scales::comma(`No. of 990 Filers w/ Gov Grants`),
    `Total Gov Grants ($)` = scales::dollar(`Total Gov Grants ($)`),
    `Operating Surplus (%)` = scales::percent(`Operating Surplus (%)`, accuracy = 0.01),
    `Operating Surplus w/o Gov Grants (%)` = scales::percent(`Operating Surplus w/o Gov Grants (%)`, accuracy = 0.01),
    `Share of 990 Filers w/ Gov Grants at Risk` = scales::percent(`Share of 990 Filers w/ Gov Grants at Risk`, accuracy = 0.01)
  )

# Make sure all counties are unique
qc_county |>
  dplyr::select(CENSUS_STATE_NAME, County) |>
  unique() |>
  nrow() == nrow(qc_county)

writexl::write_xlsx(list(
  "State" = qc_state,
  "County" = qc_county,
  "Congressional District" = qc_district,
  "Size" = qc_size,
  "Subsector" = qc_subsector
), path = "data/intermediate/qc_govtgrants.xlsx")
