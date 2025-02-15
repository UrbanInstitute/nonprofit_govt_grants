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
full_sample_proc <- full_sample_proc |>
  dplyr::mutate(AT_RISK_NUM = ifelse(PROFIT_MARGIN_NOGOVTGRANT < 0, 1, 0))


# Helper scripts
source("R/summarise_data.R")

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

gvtgrnt <- full_sample_proc |> dplyr::filter(
  !is.na(GOVERNMENT_GRANT_DOLLAR_AMOUNT)
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
    state_abbr = usdata::state2abbr(CENSUS_STATE_NAME)
  )

qc_state <- summarize_nonprofit_data(qc,
                                     group_var = "state_abbr",
                                     group_var_rename = "State_Abbr",
                                     qc = TRUE) |>
  dplyr::select(! State_Abbr)

qc_district <- summarize_nonprofit_data(full_sample_proc,
                                              group_var = "CONGRESS_DISTRICT_NAME",
                                              group_var_rename = "Congressional District",
                                              qc = TRUE)

qc_county <- summarize_nonprofit_data(full_sample_proc,
                                            group_var = "CENSUS_COUNTY_NAME",
                                            group_var_rename = "County",
                                            qc = TRUE)

qc_size <- summarize_nonprofit_data(full_sample_proc,
                                          group_var = "EXPENSE_CATEGORY",
                                          group_var_rename = "Size",
                                          qc = TRUE)

qc_subsector <- summarize_nonprofit_data(full_sample_proc,
                                               group_var = "SUBSECTOR",
                                               group_var_rename = "Subsector",
                                               qc = TRUE)

# Plotting for outliers
# Picking a couple of specific nonprofits and tracing them through
  # 

write_xlsx(list(
  "County" = national_county,
  "Congressional District" = national_district,
  "Size" = national_size,
  "Subsector" = national_subsector
), path = "data/intermediate/qc_govtgrants.xlsx")
