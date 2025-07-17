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

# Merge with financial metrics

# (3) - Load: Create output data

NY_metrics <- summarise_state_data(financial_metrics_cbsa, "New York")
FL_metrics <- summarise_state_data(financial_metrics_cbsa, "Florida")
PR_metrics <- summarise_state_data(financial_metrics_cbsa, "Puerto Rico")

writexl::write_xlsx(
  list(
    "New York" = NY_metrics,
    "Florida" = FL_metrics,
    "Puerto Rico" = PR_metrics
  ),
  path = "data/congressional_briefing_metrics-20250717.xlsx"
)

NY_full <- ny_demo |> tidylog::left_join(NY_metrics)
FL_full <- fl_demo |> tidylog::left_join(FL_metrics)
PR_full <- pr_demo |> tidylog::left_join(PR_metrics)

# TODO: 
# NP Trends Sample
# Fill NA with 0
# Rearrange by units and alphabetically
# Add United States row

###############################################################################

# Merge financial risk data

tigris_cbsa_merge <- tigris_cbsa |>
  dplyr::select(NAMELSAD, geometry) |>
  dplyr::rename(CBSA_NAME = NAMELSAD) |>
  sf::st_transform(4326)

# Spatial Join

full_sample_int <- full_sample_int |>
  sf::st_as_sf()|>
  sf::st_transform(4326) |>
  sf::st_make_valid()

full_sample_cbsa <- sf::st_join(full_sample_int,
                                tigris_cbsa_merge)

full_sample_cbsa_proc <- full_sample_cbsa |>
  dplyr::mutate(
    expense_category = dplyr::case_when(
      F9_09_EXP_TOT_TOT < 100000 ~ "Less than $100K",
      F9_09_EXP_TOT_TOT >= 100000 &
        F9_09_EXP_TOT_TOT < 500000 ~ "Between $100K and $499K",
      F9_09_EXP_TOT_TOT >= 500000 &
        F9_09_EXP_TOT_TOT < 1000000 ~ "Between $500K and $999K",
      F9_09_EXP_TOT_TOT >= 1000000 &
        F9_09_EXP_TOT_TOT < 5000000 ~ "Between $1M and $4.99M",
      F9_09_EXP_TOT_TOT >= 5000000 &
        F9_09_EXP_TOT_TOT < 10000000 ~ "Between $5M and $9.99M",
      F9_09_EXP_TOT_TOT >= 10000000 ~ "Greater than $10M",
      .default = "No Expenses Provided"
    ),
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
      .default = "Unclassified"  # Default case for unmatched codes
    )
  ) |>
  dplyr::mutate(
    CONGRESS_DISTRICT_NAME = dplyr::case_when(
      NAMELSAD == "Congressional District" ~ "Unmapped",
    )
  ) |>
  dplyr::select(
    EIN2,
    CENSUS_REGION,
    CENSUS_COUNTY_NAME,
    CENSUS_STATE_ABBR,
    CBSA_NAME,
    NAMELSAD,
    SUBSECTOR,
    expense_category,
    F9_08_REV_CONTR_GOVT_GRANT,
    profit_margin,
    profit_margin_nogovtgrant,
    at_risk
  ) |>
  dplyr::mutate(
    CENSUS_REGION = ifelse(is.na(CENSUS_REGION), "Unmapped", CENSUS_REGION),
    NAMELSAD = ifelse(is.na(NAMELSAD), "Unmapped", NAMELSAD)
  ) |>
  dplyr::rename(
    CONGRESS_DISTRICT_NAME = NAMELSAD,
    GOVERNMENT_GRANT_DOLLAR_AMOUNT = F9_08_REV_CONTR_GOVT_GRANT,
    EXPENSE_CATEGORY = expense_category,
    PROFIT_MARGIN = profit_margin,
    PROFIT_MARGIN_NOGOVTGRANT = profit_margin_nogovtgrant,
    AT_RISK_NUM = at_risk
  ) |>
  dplyr::mutate(
    EXPENSE_CATEGORY = dplyr::case_when(
      EXPENSE_CATEGORY == "Less than $100K" ~ "Less than $100K",
      EXPENSE_CATEGORY == "Between $100K and $499K" ~ "$100K to $499K",
      EXPENSE_CATEGORY == "Between $500K and $999K" ~ "$500K to $999K",
      EXPENSE_CATEGORY == "Between $1M and $4.99M" ~ "$1M to $4.9M",
      EXPENSE_CATEGORY == "Between $5M and $9.99M" ~ "$5M to $9.9M",
      EXPENSE_CATEGORY == "Greater than $10M" ~ "$10M or more",
      TRUE ~ EXPENSE_CATEGORY
    ),
    SUBSECTOR = dplyr::case_when(
      SUBSECTOR == "Arts, Culture, and Humanities" ~ "Arts, culture, and humanities",
      SUBSECTOR == "Education (Excluding Universities)" ~ "Education",
      SUBSECTOR == "Environment and Animals" ~ "Environment and animals",
      SUBSECTOR == "Health (Excluding Hospitals)" ~ "Health",
      SUBSECTOR == "Human Services" ~ "Human services",
      SUBSECTOR == "Hospitals" ~ "Hospitals",
      SUBSECTOR == "International, Foreign Affairs" ~ "International, foreign affairs",
      SUBSECTOR == "Public, Societal Benefit" ~ "Public, societal benefit",
      SUBSECTOR == "Religion Related" ~ "Religion-related",
      SUBSECTOR == "Mutual/Membership Benefit" ~ "Mutual/membership benefit",
      SUBSECTOR == "Universities" ~ "Universities",
      SUBSECTOR == "Unclassified" ~ "Unclassified",
      TRUE ~ SUBSECTOR
    )
  ) |>
  dplyr::mutate(
    SUBSECTOR = factor(
      SUBSECTOR,
      c(
        "Arts, culture, and humanities",
        "Education",
        "Environment and animals",
        "Health",
        "Hospitals",
        "Human services",
        "International, foreign affairs",
        "Public, societal benefit",
        "Religion-related",
        "Mutual/membership benefit",
        "Universities",
        "Unclassified"
      )
    )
  ) |>
  dplyr::filter(CENSUS_STATE_ABBR %in% c("NY", "FL", "PR"))


# Identify records with missing cbsa but valid geometry

null_cbsa <- full_sample_cbsa_proc |>
  dplyr::filter(is.na(CBSA_NAME), !sf::st_is_empty(geometry)) |>
  dplyr::select(EIN2, geometry) |>
  sf::st_as_sf()

null_cbsa <- sf::st_transform(null_cbsa, sf::st_crs(cbsa_boundaries))

map_null_cbsa <- sf::st_join(null_cbsa, tigris_cbsa, join = sf::st_intersects)

# All of the null geoms are not in CBSAs

nonprofit_metrics_df <- full_sample_cbsa_proc |>
  dplyr::mutate(
    CBSA_NAME = dplyr::case_when(
      is.na(CBSA_NAME) ~ "Not located in CBSA",
      .default = CBSA_NAME
    ),
  ) |>
  dplyr::select(! CENSUS_REGION) |>
  dplyr::rename(
    State = CENSUS_STATE_ABBR,
    County = CENSUS_COUNTY_NAME,
    `Congressional District` = CONGRESS_DISTRICT_NAME,
    `Metro Area` = CBSA_NAME
  ) |>
  tidyr::pivot_longer(
    cols = c(
      County, 
      `Congressional District`, 
      `Metro Area`
    ),
    names_to = "UNIT",
    values_to = "NAME"
  )

nonprofit_metrics_summarized <- nonprofit_metrics_df |>
  dplyr::group_by(State, NAME, UNIT) |>
  dplyr::summarise(
    num_990filers_govgrants = dplyr::n(),
    total_govt_grants = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT, na.rm = TRUE),
    median_profit_margin = median(PROFIT_MARGIN, na.rm = TRUE),
    median_profit_margin_no_govt_grants = median(PROFIT_MARGIN_NOGOVTGRANT, na.rm = TRUE),
    number_at_risk = sum(AT_RISK_NUM, na.rm = TRUE)
  ) |>
  dplyr::mutate(proportion_at_risk = number_at_risk / num_990filers_govgrants) |>
  dplyr::mutate(
    num_990filers_govgrants = scales::comma(num_990filers_govgrants),
    total_govt_grants = scales::dollar(total_govt_grants),
    median_profit_margin = scales::percent(median_profit_margin, accuracy = 0.01),
    median_profit_margin_no_govt_grants = scales::percent(median_profit_margin_no_govt_grants, accuracy = 0.01),
    proportion_at_risk = scales::percent(proportion_at_risk, accuracy = 0.01)
  )

nonprofit_metrics_summarized_state <- nonprofit_metrics_df |>
  dplyr::select(
    State,
    EIN2,
    GOVERNMENT_GRANT_DOLLAR_AMOUNT,
    PROFIT_MARGIN,
    PROFIT_MARGIN_NOGOVTGRANT,
    AT_RISK_NUM
  ) |>
  dplyr::distinct() |>
  dplyr::group_by(State) |>
  dplyr::summarise(
    num_990filers_govgrants = dplyr::n(),
    total_govt_grants = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT, na.rm = TRUE),
    median_profit_margin = median(PROFIT_MARGIN, na.rm = TRUE),
    median_profit_margin_no_govt_grants = median(PROFIT_MARGIN_NOGOVTGRANT, na.rm = TRUE),
    number_at_risk = sum(AT_RISK_NUM, na.rm = TRUE)
  ) |>
  dplyr::mutate(proportion_at_risk = number_at_risk / num_990filers_govgrants) |>
  dplyr::mutate(
    num_990filers_govgrants = scales::comma(num_990filers_govgrants),
    total_govt_grants = scales::dollar(total_govt_grants),
    median_profit_margin = scales::percent(median_profit_margin, accuracy = 0.01),
    median_profit_margin_no_govt_grants = scales::percent(median_profit_margin_no_govt_grants, accuracy = 0.01),
    proportion_at_risk = scales::percent(proportion_at_risk, accuracy = 0.01)
  ) |>
  dplyr::mutate(
    NAME = dplyr::case_when(
      State == "NY" ~ "New York",
      State == "FL" ~ "Florida",
      State == "PR" ~ "Puerto Rico",
      TRUE ~ "Unmapped"
    ),
    UNIT = "State"
  )

nonprofit_metrics_summarized <- dplyr::bind_rows(
  nonprofit_metrics_summarized, 
  nonprofit_metrics_summarized_state
)

nonprofit_metrics_summarized <- nonprofit_metrics_summarized |>
  dplyr::rename(
    STATE = State
  )

ny_metrics <- sf::st_drop_geometry(ny_full) |>
  tidylog::left_join(
    sf::st_drop_geometry(nonprofit_metrics_summarized),
    by = c("NAME", "STATE", "UNIT")
  )

fl_metrics <- sf::st_drop_geometry(fl_full) |>
  tidylog::left_join(
    sf::st_drop_geometry(nonprofit_metrics_summarized),
    by = c("NAME", "STATE", "UNIT")
  )

pr_metrics <- sf::st_drop_geometry(pr_full) |>
  tidylog::left_join(
    sf::st_drop_geometry(nonprofit_metrics_summarized),
    by = c("NAME", "STATE", "UNIT")
  )

writexl::write_xlsx(
  list(
    "New York" = ny_metrics,
    "Florida" = fl_metrics,
    "Puerto Rico" = pr_metrics
  ),
  path = "data/congressional_briefing-20250715-nonprofit_metrics.xlsx"
)