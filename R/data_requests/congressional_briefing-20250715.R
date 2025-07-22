#-------------------------------------------------------------------------------
#------------------------------------------------------------------------------


# Helper scripts
source(here::here("R", "data_requests", "congressional_briefing_utils-20250715.R"))

# (1) - Extract

# (1.1) - Nonprofit Financial Metrics

nonprofit_financial_metrics <- data.table::fread("data/processed/full_sample_processed_v1.0.csv") |>
  dplyr::select(
    EIN2,
    CENSUS_STATE_NAME,
    CENSUS_COUNTY_NAME,
    CONGRESS_DISTRICT_NAME,
    GOVERNMENT_GRANT_DOLLAR_AMOUNT,
    PROFIT_MARGIN,
    PROFIT_MARGIN_NOGOVTGRANT,
    AT_RISK_NUM
  )

# (1.2) - BMF
bmf_sample <- data.table::fread("data/raw/unified_bmf.csv") |>
  dplyr::filter(EIN2 %in% nonprofit_financial_metrics$EIN2) |>
  dplyr::group_by(EIN2) |>
  dplyr::arrange(ORG_YEAR_LAST) |>
  dplyr::slice_max(ORG_YEAR_LAST)

bmf_sf <- bmf_sample |>
  sf::st_as_sf(coords = c("LONGITUDE", "LATITUDE"), crs = 4269) |>
  sf::st_make_valid()
  
# (1.3) - Tigris Census Units
tigris_cbsa <- tigris::core_based_statistical_areas(cb = TRUE)
tigris_states <- tigris::states(cb = TRUE)

# State-level tigris census geographies
ny_geos <- state_geos("NY", tigris_states, tigris_cbsa)
fl_geos <- state_geos("FL", tigris_states, tigris_cbsa)
pr_geos <- state_geos("PR", tigris_states, tigris_cbsa)

# (1.4) - ACS Demographic Data

acs_states <- c("NY", "FL", "PR")
acs_geounits <- c("congressional district", "county", "state")

# State-level Population Counts
acs_pop_df <- purrr::map(acs_states, function(state) {
  purrr::map(acs_geounits, get_pop_counts, state = state) |>
    purrr::list_rbind() |>
    dplyr::mutate(STATE = state)
}, .progress = TRUE) |>
  purrr::list_rbind() |>
  dplyr::select(!moe) |>
  tidyr::pivot_wider(names_from = variable, values_from = estimate) |>
  dplyr::rename(TOTAL_POPULATION = B03001_001,
                TOTAL_HISPANIC_LATINO_POPULATION = B03001_003) |>
  dplyr::mutate(
    NAME = gsub(" \\(118th Congress\\)", "", NAME),
    NAME = gsub(", New York|, Florida|, Puerto Rico", "", NAME)
  )

# Metro-Area Populaion Counts
acs_metro_areas <- tidycensus::get_acs(
  geography = "metropolitan statistical area/micropolitan statistical area",
  variables = c("B03001_001", "B03001_003"),
  survey = "acs5",
  year = 2023,
  cache_table = TRUE
) |>
  dplyr::select(! moe) |>
  dplyr::filter(grepl("Metro Area", NAME)) |>
  dplyr::filter(grepl("NY|FL|PR", NAME)) |>
  tidyr::pivot_wider(
    names_from = variable,
    values_from = estimate
  ) |>
  dplyr::rename(
    TOTAL_POPULATION = B03001_001,
    TOTAL_HISPANIC_LATINO_POPULATION = B03001_003
  ) |>
  dplyr::mutate(
    STATE = dplyr::case_when(
      grepl("NY", NAME) ~ "NY",
      grepl("FL", NAME) ~ "FL",
      grepl("PR", NAME) ~ "PR",
      .default = "Unmapped"
    )
  )

# State-level combined ACS and tigris data
ny_demo <- get_state_demographics(acs_pop_df, acs_metro_areas, ny_geos, state_abbr = "NY")
fl_demo <- get_state_demographics(acs_pop_df, acs_metro_areas, fl_geos, state_abbr = "FL")
pr_demo <- get_state_demographics(acs_pop_df, acs_metro_areas, pr_geos, state_abbr = "PR")
usa_demo <- tidycensus::get_acs(
  geography = "us",
  variables = c("B03001_001", "B03001_003"),
  survey = "acs5",
  year = 2023,
  cache_table = TRUE
) |>
  dplyr::select(! moe) |>
  tidyr::pivot_wider(
    names_from = variable,
    values_from = estimate
  ) |>
  dplyr::rename(
    TOTAL_POPULATION = B03001_001,
    TOTAL_HISPANIC_LATINO_POPULATION = B03001_003
  ) |>
  dplyr::mutate(
    PERCENT_HISPANIC_LATINO = round(
      TOTAL_HISPANIC_LATINO_POPULATION / TOTAL_POPULATION,
      2
    ) * 100,
  ) |>
  dplyr::mutate(
    STATE = "US",
    UNIT = "NATIONAL"
  ) |>
  dplyr::select(
    NAME, STATE, UNIT,
    TOTAL_POPULATION, 
    TOTAL_HISPANIC_LATINO_POPULATION,
    PERCENT_HISPANIC_LATINO
  )


# (1.5) - Save Intermediate datasets

# Save as separate sheets in an xlsx workbook
writexl::write_xlsx(
  list(
    "New York" = ny_demo,
    "Florida" = fl_demo,
    "Puerto Rico" = pr_demo
  ),
  path = "data/congressional_briefing_demographics-20250717.xlsx"
)

# (2) - Transform

# (2.1) - Merge CBSA with Unified BMF

bmf_cbsa <- bmf_sf |>
  dplyr::select(EIN2, geometry) |>
  dplyr::group_by(EIN2) |>
  dplyr::slice(1) |>
  sf::st_join(tigris_cbsa, join = sf::st_intersects) |>
  dplyr::mutate(
    CBSA_NAME = dplyr::case_when(
      is.na(NAMELSAD) ~ "Not located in CBSA",
      .default = NAMELSAD
    )
  ) |>
  dplyr::select(EIN2, CBSA_NAME) |>
  sf::st_drop_geometry()

# Merge with financial metrics
financial_metrics_cbsa <- nonprofit_financial_metrics |>
  tidylog::left_join(bmf_cbsa, by = "EIN2")

# (2.2) - Isolate PR Nonprofits in BMF and add to financial metrics

bmf_pr_eins <- bmf_sf |>
  dplyr::select(EIN2, geometry) |>
  sf::st_join(tigris_states, join = sf::st_intersects) |>
  dplyr::filter(STUSPS == "PR") |>
  dplyr::pull(EIN2)

financial_metrics_cbsa <- financial_metrics_cbsa |>
  dplyr::mutate(
    CENSUS_STATE_NAME = dplyr::case_when(
      EIN2 %in% bmf_pr_eins ~ "Puerto Rico",
      .default = CENSUS_STATE_NAME
    )
  )

# (2.3) - Create NP Trends Sample in BMF
nptrends_sample_eins <- bmf_sf |>
  dplyr::filter(
     NTEE_IRS %in% ntee_irs_incl
  ) |>
  dplyr::pull(EIN2)


nptrends_sample <- financial_metrics_cbsa |>
  dplyr::filter(EIN2 %in% nptrends_sample_eins)

# Merge with financial metrics

# (3) - Load: Create output data

NY_metrics <- summarise_state_data(financial_metrics_cbsa, "New York")
NY_metrics_nptrends <- summarise_state_data(nptrends_sample, "New York")
FL_metrics <- summarise_state_data(financial_metrics_cbsa, "Florida")
FL_metrics_nptrends <- summarise_state_data(nptrends_sample, "Florida")
PR_metrics <- summarise_state_data(financial_metrics_cbsa, "Puerto Rico")
PR_metrics_nptrends <- summarise_state_data(nptrends_sample, "Puerto Rico")

usa_metrics <- financial_metrics_cbsa |>
  dplyr::summarise(
    num_990filers_govgrants = dplyr::n(),
    total_govt_grants = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT, na.rm = TRUE),
    median_profit_margin = round(median(PROFIT_MARGIN, na.rm = TRUE), 3) * 100,
    median_profit_margin_no_govt_grants = round(median(PROFIT_MARGIN_NOGOVTGRANT, na.rm = TRUE), 3) * 100,
    number_at_risk = sum(AT_RISK_NUM, na.rm = TRUE)
  ) |>
  dplyr::mutate(proportion_at_risk = round(number_at_risk / num_990filers_govgrants, 3) * 100) |>
  dplyr::select(! number_at_risk) |>
  dplyr::mutate(
    NAME = "United States"
  )
usa_metrics_nptrends <- nptrends_sample |>
  dplyr::summarise(
    num_990filers_govgrants = dplyr::n(),
    total_govt_grants = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT, na.rm = TRUE),
    median_profit_margin = round(median(PROFIT_MARGIN, na.rm = TRUE), 3) * 100,
    median_profit_margin_no_govt_grants = round(median(PROFIT_MARGIN_NOGOVTGRANT, na.rm = TRUE), 3) * 100,
    number_at_risk = sum(AT_RISK_NUM, na.rm = TRUE)
  ) |>
  dplyr::mutate(proportion_at_risk = round(number_at_risk / num_990filers_govgrants, 3) * 100) |>
  dplyr::select(! number_at_risk) |>
  dplyr::mutate(
    NAME = "United States"
  )

usa_metrics_full <- usa_demo |>
  tidylog::left_join(usa_metrics, by = "NAME") |>
  tidylog::left_join(usa_metrics_nptrends, by = "NAME", suffix = c("", "_NPTrends")) |>
  dplyr::mutate(
    UNIT = "NATIONAL",
    STATE = "US"
  ) |>
  dplyr::select(
    NAME, 
    STATE, 
    UNIT,
    TOTAL_POPULATION, 
    TOTAL_HISPANIC_LATINO_POPULATION,
    PERCENT_HISPANIC_LATINO,
    num_990filers_govgrants,
    total_govt_grants,
    median_profit_margin,
    median_profit_margin_no_govt_grants,
    proportion_at_risk,
    num_990filers_govgrants_NPTrends,
    total_govt_grants_NPTrends,
    median_profit_margin_NPTrends,
    median_profit_margin_no_govt_grants_NPTrends,
    proportion_at_risk_NPTrends
  )

writexl::write_xlsx(
  list(
    "New York" = NY_metrics,
    "Florida" = FL_metrics,
    "Puerto Rico" = PR_metrics
  ),
  path = "data/congressional_briefing_metrics-20250717.xlsx"
)

NY_full <- usa_metrics_full |>
  dplyr::bind_rows(
    ny_demo |> 
      tidylog::left_join(NY_metrics, by = "NAME") |>
      tidylog::left_join(NY_metrics_nptrends, by = "NAME", suffix = c("", "_NPTrends"))
  )

FL_full <- usa_metrics_full |>
  dplyr::bind_rows(
    fl_demo |> 
      tidylog::left_join(FL_metrics, by = "NAME") |>
      tidylog::left_join(FL_metrics_nptrends, by = "NAME", suffix = c("", "_NPTrends"))
  )
  
PR_full <- usa_metrics_full |>
  dplyr::bind_rows(
    pr_demo |>
      tidylog::left_join(PR_metrics, by = "NAME") |>
      tidylog::left_join(
        PR_metrics_nptrends,
        by = "NAME",
        suffix = c("", "_NPTrends")
      )
  ) |>
  dplyr::mutate(across(everything(), ~tidyr::replace_na(., 0)))

writexl::write_xlsx(
  list(
    "New York" = NY_full,
    "Florida" = FL_full,
    "Puerto Rico" = PR_full
  ),
  path = "data/congressional_briefing_metrics_full-20250717.xlsx"
)
