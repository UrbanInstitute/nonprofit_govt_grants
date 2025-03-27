rename_list <- list(
  "Number of 990 filers with government grants" = "num_990filers_govgrants",
  "Total government grants ($)" = "total_govt_grants",
  "Operating surplus with government grants (%)" = "median_profit_margin",
  "Operating surplus without government grants (%)" = "median_profit_margin_no_govt_grants",
  "Share of 990 filers with government grants at risk" = "proportion_at_risk",
  "Number of 990 filers with government grants at risk" = "number_at_risk"
)

ntee_codes <- readxl::read_xlsx("data/raw/ntee_codes.xlsx") |>
  dplyr::rename(NTEEV2 = "NTEEV2 Code (New NTEE Code)")

ct_hsng_tot <- full_sample_proc |>
  dplyr::summarise(
    num_990filers_govgrants = dplyr::n(),
    total_govt_grants = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT, na.rm = TRUE),
    median_profit_margin = median(PROFIT_MARGIN, na.rm = TRUE),
    median_profit_margin_no_govt_grants = median(PROFIT_MARGIN_NOGOVTGRANT, na.rm = TRUE),
    number_at_risk = sum(AT_RISK_NUM, na.rm = TRUE)
  ) |>
  dplyr::mutate(proportion_at_risk = number_at_risk / num_990filers_govgrants) |>
  dplyr::rename(!!!rename_list)

ct_county <- full_sample_proc |>
  group_by(CENSUS_COUNTY_NAME) |>
  dplyr::summarise(
    num_990filers_govgrants = dplyr::n(),
    total_govt_grants = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT, na.rm = TRUE),
    median_profit_margin = median(PROFIT_MARGIN, na.rm = TRUE),
    median_profit_margin_no_govt_grants = median(PROFIT_MARGIN_NOGOVTGRANT, na.rm = TRUE),
    number_at_risk = sum(AT_RISK_NUM, na.rm = TRUE)
  ) |>
  dplyr::mutate(proportion_at_risk = number_at_risk / num_990filers_govgrants) |>
  dplyr::rename(!!!rename_list,
                "County" = "CENSUS_COUNTY_NAME")

ct_district <- full_sample_proc |>
  group_by(CONGRESS_DISTRICT_NAME) |>
  dplyr::summarise(
    num_990filers_govgrants = dplyr::n(),
    total_govt_grants = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT, na.rm = TRUE),
    median_profit_margin = median(PROFIT_MARGIN, na.rm = TRUE),
    median_profit_margin_no_govt_grants = median(PROFIT_MARGIN_NOGOVTGRANT, na.rm = TRUE),
    number_at_risk = sum(AT_RISK_NUM, na.rm = TRUE)
  ) |>
  dplyr::mutate(proportion_at_risk = number_at_risk / num_990filers_govgrants) |>
  dplyr::rename(!!!rename_list,
                "Congressional District" = "CONGRESS_DISTRICT_NAME")

ct_size <- full_sample_proc |>
  group_by(EXPENSE_CATEGORY) |>
  dplyr::summarise(
    num_990filers_govgrants = dplyr::n(),
    total_govt_grants = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT, na.rm = TRUE),
    median_profit_margin = median(PROFIT_MARGIN, na.rm = TRUE),
    median_profit_margin_no_govt_grants = median(PROFIT_MARGIN_NOGOVTGRANT, na.rm = TRUE),
    number_at_risk = sum(AT_RISK_NUM, na.rm = TRUE)
  ) |>
  dplyr::mutate(proportion_at_risk = number_at_risk / num_990filers_govgrants) |>
  dplyr::rename(!!!rename_list,
                "Size" = "EXPENSE_CATEGORY")

ct_ntee <- full_sample_proc |>
  group_by(NTEEV2) |>
  dplyr::summarise(
    num_990filers_govgrants = dplyr::n(),
    total_govt_grants = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT, na.rm = TRUE),
    median_profit_margin = median(PROFIT_MARGIN, na.rm = TRUE),
    median_profit_margin_no_govt_grants = median(PROFIT_MARGIN_NOGOVTGRANT, 
                                                 na.rm = TRUE),
    number_at_risk = sum(AT_RISK_NUM, na.rm = TRUE)
  ) |>
  dplyr::mutate(proportion_at_risk = number_at_risk / num_990filers_govgrants) |>
  right_join(ntee_codes, by = "NTEEV2") |>
  tidyr::replace_na(list(num_990filers_govgrants = 0,
                           total_govt_grants = 0,
                           median_profit_margin = 0,
                           median_profit_margin_no_govt_grants = 0,
                           number_at_risk = 0,
                           proportion_at_risk = 0)) |>
  dplyr::rename(!!!rename_list,
                "NTEE Code" = "NTEEV2")

writexl::write_xlsx(
  list(
    "Total" = ct_hsng_tot,
    "By county" = ct_county,
    "By district" = ct_district,
    "By size" = ct_size,
    "By ntee" = ct_ntee
  ),
  "data/intermediate/psc_032725.xlsx"
)
