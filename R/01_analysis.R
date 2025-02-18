# Script Header
# Title: Federal Funding Freeze Blog Post
# Date created: 2025-02-03
# Date last modified: 2025-02-17
# Description: This script contains code to analyze data for HTML fact sheets 
# on nonprofits's fiscal sustainability and reliance on government grants for 
# Tax Year 2021. It creates data.frames for fact sheets for each disaggregation.

# Packages
library(tidyverse)
library(data.table)
library(purrr)
library(tidyr)

# Scripts
source("R/summarise_data.R")

# Load in data
full_sample_proc <- data.table::fread("data/intermediate/full_sample_processed.csv")
absent_counties <- data.table::fread("data/intermediate/absent_counties.csv")

# (1) - National Summaries
national_bystate <- full_sample_proc |>
  dplyr::group_by(CENSUS_STATE_NAME) |>
  dplyr::summarise(
    num_990filers_govgrants = dplyr::n(),
    total_govt_grants = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT, na.rm = TRUE),
    median_profit_margin = median(PROFIT_MARGIN, na.rm = TRUE),
    median_profit_margin_no_govt_grants = median(PROFIT_MARGIN_NOGOVTGRANT, na.rm = TRUE),
    number_at_risk = sum(AT_RISK_NUM, na.rm = TRUE),
    mcoh_less3 = sum(MONTHS_CASH_ON_HAND < 3, na.rm = TRUE),
    mcoh_less3_jesse = sum(MONTHS_CASH_ON_HAND_JESSE < 3, na.rm = TRUE),
    operating_reserve_ratio = mean(OPERATING_RESERVE_RATIO < 0.5, na.rm = TRUE)
  ) |>
  dplyr::mutate(proportion_at_risk = number_at_risk / num_990filers_govgrants, ) |>
  dplyr::select(
    CENSUS_STATE_NAME,
    num_990filers_govgrants,
    total_govt_grants,
    median_profit_margin,
    median_profit_margin_no_govt_grants,
    proportion_at_risk,
    mcoh_less3,
    mcoh_less3_jesse,
    operating_reserve_ratio
  ) |>
  dplyr::mutate(
    total_govt_grants = scales::dollar(total_govt_grants),
    median_profit_margin = scales::percent(median_profit_margin, accuracy = 0.01),
    median_profit_margin_no_govt_grants = scales::percent(median_profit_margin_no_govt_grants, accuracy = 0.01),
    proportion_at_risk = scales::percent(proportion_at_risk, accuracy = 0.01),
    mcoh_less3 = scales::percent(mcoh_less3 / num_990filers_govgrants, accuracy = 0.01),
    mcoh_less3_jesse = scales::percent(mcoh_less3_jesse / num_990filers_govgrants, accuracy = 0.01),
    operating_reserve_ratio = scales::percent(operating_reserve_ratio, accuracy = 0.01)
  ) |.
  dplyr::rename(
    "State" = CENSUS_STATE_NAME,
    "Number of 990 Filers reporting Government Grants" = num_990filers_govgrants,
    "Total Government Grants ($USD)" = total_govt_grants,
    "Median Operating Surplus (%)" = median_profit_margin,
    "Median Operating Surplus (Without Government Grants) (%)" = median_profit_margin_no_govt_grants,
    "Proportion of Nonprofits at Risk" = proportion_at_risk,
    "% With Less than 3 Months Cash on Hand" = mcoh_less3,
    "% With Less than 3 Months Cash on Hand (Jesse)" = mcoh_less3_jesse,
    "Operating Reserve Ratio < 0.5" = operating_reserve_ratio
  )

national_bystate <- summarize_nonprofit_data(full_sample_proc,
                                             group_var = "CENSUS_STATE_NAME",
                                             group_var_rename = "State")
national_bysize <- summarize_nonprofit_data(full_sample_proc,
                                           group_var = "EXPENSE_CATEGORY",
                                           group_var_rename = "Size")

national_bysubsector <- summarize_nonprofit_data(full_sample_proc,
                                                 group_var = "SUBSECTOR",
                                                 group_var_rename = "Subsector")
  
national <- summarize_nonprofit_data(full_sample_proc)

datasets <- list(
  "national" = national,
  "national_bystate" = national_bystate,
  "national_bysize" = national_bysize,
  "national_bysubsector" = national_bysubsector
)

purrr::walk2(
  datasets,
  names(datasets),
  function(df, name) {
    data.table::fwrite(df, paste0("data/intermediate/national_thiya_", name, ".csv"))
  }
)

library(writexl)
write_xlsx(list(
  "National" = national,
  "State" = national_bystate,
  "Size" = national_bysize,
  "Subsector" = national_bysubsector
), path = "data/intermediate/national_thiya.xlsx")

# (2) - Massachussets Summary

ma_sample <- full_sample_proc |>
  dplyr::filter(CENSUS_STATE_NAME == "Massachusetts")

ma_overall <- summarize_nonprofit_data(ma_sample)

ma_bycounty <- summarize_nonprofit_data(ma_sample,
                                        group_var = "CENSUS_COUNTY_NAME",
                                        group_var_rename = "County")

ma_bydistrict <- summarize_nonprofit_data(ma_sample,
                                          group_var = "CONGRESS_DISTRICT_NAME",
                                          group_var_rename = "Congressional District")

ma_bysize <- summarize_nonprofit_data(ma_sample,
                                      group_var = "EXPENSE_CATEGORY",
                                      group_var_rename = "Size")

ma_bysubsector <- summarize_nonprofit_data(ma_sample,
                                           group_var = "SUBSECTOR",
                                           group_var_rename = "Subsector")

library(writexl)
write_xlsx(list(
  "Massachusetts" = ma_overall,
  "County" = ma_bycounty,
  "Congressional District" = ma_bydistrict,
  "Size" = ma_bysize,
  "Subsector" = ma_bysubsector
), path = "data/intermediate/ma_govtgrants.xlsx")

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
                                "Other US Jurisdictions", 
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
