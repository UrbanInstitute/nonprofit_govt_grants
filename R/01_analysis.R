# Load in data
full_sample_proc <- data.table::fread("data/intermediate/full_sample_processed.csv")


# Sample Arizona - By Subsector

full_sample_proc |>
  dplyr::filter(CENSUS_STATE_ABBR == "AZ",
                TANGIBLE_ASSETS_REPORTED != "") |> # parameter 1 - geography
  dplyr::group_by(EXPENSE_CATEGORY) |> # parameter 2 - aggregation
  dplyr::summarise(
    mean_months_cash_on_hand_tangibleassets = mean(ifelse(TANGIBLE_ASSETS_REPORTED == "Y", MONTHS_CASH_ON_HAND, NA), na.rm = TRUE),
    median_months_cash_on_hand_tangibleassets = median(ifelse(TANGIBLE_ASSETS_REPORTED == "Y", MONTHS_CASH_ON_HAND, NA), na.rm = TRUE),
    mean_months_cash_on_hand_notangibleassets = mean(ifelse(TANGIBLE_ASSETS_REPORTED == "N", MONTHS_CASH_ON_HAND, NA), na.rm = TRUE),
    median_months_cash_on_hand_notangibleassets = median(ifelse(TANGIBLE_ASSETS_REPORTED == "N", MONTHS_CASH_ON_HAND, NA), na.rm = TRUE),
    proportion_govt_grants_20 = dplyr::n_distinct(ifelse(PROPORTION_GOVT_GRANT >= 0 & PROPORTION_GOVT_GRANT <= 0.2, EIN2, NA)),
    proportion_govt_grants_40 = dplyr::n_distinct(ifelse(PROPORTION_GOVT_GRANT > 0.2 & PROPORTION_GOVT_GRANT <= 0.4, EIN2, NA)),
    proportion_govt_grants_60 = dplyr::n_distinct(ifelse(PROPORTION_GOVT_GRANT > 0.4 & PROPORTION_GOVT_GRANT <= 0.6, EIN2, NA)),
    proportion_govt_grants_80 = dplyr::n_distinct(ifelse(PROPORTION_GOVT_GRANT > 0.6 & PROPORTION_GOVT_GRANT <= 0.8, EIN2, NA)),
    proportion_govt_grants_100 = dplyr::n_distinct(ifelse(PROPORTION_GOVT_GRANT > 0.8 & PROPORTION_GOVT_GRANT <= 1, EIN2, NA)),
    total_govt_grants = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT, na.rm = TRUE),
    total_number_nonprofits = dplyr::n_distinct(EIN2)
  ) |>
  dplyr::mutate(
    dplyr::across(proportion_govt_grants_20:proportion_govt_grants_100, ~.x / total_number_nonprofits * 100)
  ) |>
  dplyr::mutate(
    dplyr::across(proportion_govt_grants_20:proportion_govt_grants_100, round, 2)
  ) |>
  View()


