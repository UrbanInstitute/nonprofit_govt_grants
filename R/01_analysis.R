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

# Pull Massachusetts data for testing

# Massachussets by County

ma_county <- full_sample_proc |>
  dplyr::filter(CENSUS_STATE_ABBR == "MA") |>
  dplyr::group_by(CENSUS_COUNTY_NAME) |>
  dplyr::summarise(
    number_reporting_govt_grants = dplyr::n(),
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
  ) |>
  dplyr::select(
    CENSUS_COUNTY_NAME,
    number_reporting_govt_grants,
    total_govt_grants,
    median_profit_margin,
    median_profit_margin_no_govt_grants,
    median_days_cash_on_hand,
    lessthan30days_cash_on_hand,
    btwn30and90days_cash_on_hand,
    morethan90days_cash_on_hand,
    median_months_cash_on_hand,
    lessthan1month_cash_on_hand,
    btwn1and3months_cash_on_hand,
    morethan3months_cash_on_hand
  ) |>
  dplyr::mutate(
    number_reporting_govt_grants = scales::number(number_reporting_govt_grants),
    total_govt_grants = scales::dollar(total_govt_grants),
    median_profit_margin = scales::percent(median_profit_margin, accuracy = 0.01),
    median_profit_margin_no_govt_grants = scales::percent(median_profit_margin_no_govt_grants,
                                                          accuracy = 0.01),
    
  )

numnonprofits_county <- bmf_sample |>
  dplyr::filter(CENSUS_STATE_ABBR == "MA",
               ORG_YEAR_LAST >= 2021) |>
  dplyr::group_by(CENSUS_COUNTY_NAME) |>
  dplyr::summarise(num_nonprofits = dplyr::n_distinct(EIN2)) |>
  sf::st_drop_geometry()

ma_county <- tidylog::left_join(ma_county, numnonprofits_county, by = "CENSUS_COUNTY_NAME")

ma_county <- ma_county |>
  dplyr::mutate(
    total_govt_grants = scales::dollar(total_govt_grants),
    median_profit_margin = scales::percent(median_profit_margin, accuracy = 0.01),
    median_profit_margin_no_govt_grants = scales::percent(median_profit_margin_no_govt_grants,
                                                          accuracy = 0.01),
    median_days_cash_on_hand = scales::number(median_days_cash_on_hand),
    median_months_cash_on_hand = scales::number(median_months_cash_on_hand),
    lessthan30days_cash_on_hand = scales::percent(lessthan30days_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01),
    btwn30and90days_cash_on_hand = scales::percent(btwn30and90days_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01),
    morethan90days_cash_on_hand = scales::percent(morethan90days_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01),
    lessthan1month_cash_on_hand = scales::percent(lessthan1month_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01),
    btwn1and3months_cash_on_hand = scales::percent(btwn1and3months_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01),
    morethan3months_cash_on_hand = scales::percent(morethan3months_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01)
    
  ) |> 
  dplyr::select(
    CENSUS_COUNTY_NAME,
    num_nonprofits,
    number_reporting_govt_grants,
    total_govt_grants,
    median_profit_margin,
    median_profit_margin_no_govt_grants,
    median_days_cash_on_hand,
    lessthan30days_cash_on_hand,
    btwn30and90days_cash_on_hand,
    morethan90days_cash_on_hand,
    median_months_cash_on_hand,
    lessthan1month_cash_on_hand,
    btwn1and3months_cash_on_hand,
    morethan3months_cash_on_hand
  ) |>
  dplyr::rename(
    "County Name" = CENSUS_COUNTY_NAME,
    "Total Number of Nonprofits" = num_nonprofits,
    "Number of 990 Filers reporting Government Grants" = number_reporting_govt_grants,
    "Total Government Grants ($USD)" = total_govt_grants,
    "Median Profit Margin (%)" = median_profit_margin,
    "Median Profit Margin (Without Government Grants) (%)" = median_profit_margin_no_govt_grants,
    "Median Days Cash on Hand" = median_days_cash_on_hand,
    "% With Less than 30 Days Cash on Hand" = lessthan30days_cash_on_hand,
    "% With Between 30 and 90 Days Cash on Hand" = btwn30and90days_cash_on_hand,
    "% With More than 90 Days Cash on Hand" = morethan90days_cash_on_hand,
    "Median Months Cash on Hand" = median_months_cash_on_hand,
    "% With Less than 1 Month Cash on Hand" = lessthan1month_cash_on_hand,
    "% With Between 1 and 3 Months Cash on Hand" = btwn1and3months_cash_on_hand,
    "% With More than 3 Months Cash on Hand" = morethan3months_cash_on_hand
  )

# MA by Congressional district

ma_county <- full_sample_proc |>
  dplyr::filter(CENSUS_STATE_ABBR == "MA") |>
  dplyr::group_by(CENSUS_COUNTY_NAME) |>
  dplyr::summarise(
    number_reporting_govt_grants = dplyr::n(),
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

ma_county <- ma_county |>
  dplyr::mutate(
    total_govt_grants = scales::dollar(total_govt_grants),
    median_profit_margin = scales::percent(median_profit_margin, accuracy = 0.01),
    median_profit_margin_no_govt_grants = scales::percent(median_profit_margin_no_govt_grants,
                                                          accuracy = 0.01),
    median_days_cash_on_hand = scales::number(median_days_cash_on_hand),
    median_months_cash_on_hand = scales::number(median_months_cash_on_hand),
    lessthan30days_cash_on_hand = scales::percent(lessthan30days_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01),
    btwn30and90days_cash_on_hand = scales::percent(btwn30and90days_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01),
    morethan90days_cash_on_hand = scales::percent(morethan90days_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01),
    lessthan1month_cash_on_hand = scales::percent(lessthan1month_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01),
    btwn1and3months_cash_on_hand = scales::percent(btwn1and3months_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01),
    morethan3months_cash_on_hand = scales::percent(morethan3months_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01)
    
  ) |> 
  dplyr::select(
    CENSUS_COUNTY_NAME,
    num_nonprofits,
    number_reporting_govt_grants,
    total_govt_grants,
    median_profit_margin,
    median_profit_margin_no_govt_grants,
    median_days_cash_on_hand,
    lessthan30days_cash_on_hand,
    btwn30and90days_cash_on_hand,
    morethan90days_cash_on_hand,
    median_months_cash_on_hand,
    lessthan1month_cash_on_hand,
    btwn1and3months_cash_on_hand,
    morethan3months_cash_on_hand
  ) |>
  dplyr::rename(
    "County Name" = CENSUS_COUNTY_NAME,
    "Total Number of Nonprofits" = num_nonprofits,
    "Number of 990 Filers reporting Government Grants" = number_reporting_govt_grants,
    "Total Government Grants ($USD)" = total_govt_grants,
    "Median Profit Margin (%)" = median_profit_margin,
    "Median Profit Margin (Without Government Grants) (%)" = median_profit_margin_no_govt_grants,
    "Median Days Cash on Hand" = median_days_cash_on_hand,
    "% With Less than 30 Days Cash on Hand" = lessthan30days_cash_on_hand,
    "% With Between 30 and 90 Days Cash on Hand" = btwn30and90days_cash_on_hand,
    "% With More than 90 Days Cash on Hand" = morethan90days_cash_on_hand,
    "Median Months Cash on Hand" = median_months_cash_on_hand,
    "% With Less than 1 Month Cash on Hand" = lessthan1month_cash_on_hand,
    "% With Between 1 and 3 Months Cash on Hand" = btwn1and3months_cash_on_hand,
    "% With More than 3 Months Cash on Hand" = morethan3months_cash_on_hand
  )

bmf_sample <- bmf_sample |>
  dplyr::rename(
    "CONGRESS_DISTRICT_NAME" = NAMELSAD20
  )

ma_county <- summarize_by_region("CENSUS_COUNTY_NAME") |>
  dplyr::rename(
    "County Name" = `Region Name`
  )

ma_congress <- summarize_by_region("CONGRESS_DISTRICT_NAME") |>
  dplyr::rename(
    "Congressional District" = `Region Name`
  )

bmf_sample <- bmf_sample |>
  dplyr::select(! EXPENSE_CATEGORY)

bmf_sample <- bmf_sample |>
  tidylog::left_join(
    soi_sample)

bmf_sample |>
  dplyr::filter(CENSUS_STATE_ABBR == "MA",
                ORG_YEAR_LAST >= 2021) |>
  dplyr::group_by(EXPENSE_CATEGORY) |>
  dplyr::summarise(num_nonprofits = dplyr::n())

ma_size <- summarize_by_region("EXPENSE_CATEGORY") |>
  dplyr::rename(
    "Size" = `Region Name`
  )

bmf_sample <- bmf_sample |>
  dplyr::mutate(
    SUBSECTOR = dplyr::case_when(
      SUBSECTOR == "ART" ~ "Arts, Culture, and Humanities",
      SUBSECTOR == "EDU" ~ "Education (Excluding Universities)",
      SUBSECTOR == "ENV" ~ "Environment and Animals",
      SUBSECTOR == "HEL" ~ "Health (Excluding Hospitals)",
      SUBSECTOR == "HMS" ~ "Human Services",
      SUBSECTOR == "IFA" ~ "International, Foreign Affairs",
      SUBSECTOR == "PSB" ~ "Public, Societal Benefit",
      SUBSECTOR == "REL" ~ "Religion Related",
      SUBSECTOR == "MMB" ~ "Mutual/Membership Benefit",
      SUBSECTOR == "UNU" ~ "Unclassified",
      SUBSECTOR == "UNI" ~ "Universities",
      SUBSECTOR == "HOS" ~ "Hospitals",
      TRUE ~ "Unclassified"  # Default case for unmatched codes
    )
  )

ma_subsector <- summarize_by_region("SUBSECTOR") |>
  dplyr::rename(
    "Subsector" = `Region Name`
  )

ma <- summarize_state()

datasets <- list(
  "ma" = ma,
  "ma_county" = ma_county,
  "ma_congress" = ma_congress,
  "ma_size" = ma_size,
  "ma_subsector" = ma_subsector
)

purrr::walk2(
  datasets,
  c("state", "county", "congress", "size", "subsector"),
  function(df, name) {
    data.table::fwrite(df, paste0("data/intermediate/ma_", name, ".csv"))
  }
)


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