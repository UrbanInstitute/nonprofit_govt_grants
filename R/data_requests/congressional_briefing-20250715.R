# Create 3 separate tables for each state

# Get official geographies

# All
tigris_cbsa <- tigris::core_based_statistical_areas(cb = TRUE)
states <- tigris::states(cb = TRUE)

# New York
ny_state <- states |> 
  dplyr::filter(STUSPS == "NY") |>
  dplyr::select(NAME) |>
  dplyr::mutate(STATE = "NY",
                UNIT = "State")
## Tigris
### Congressional Districts
ny_tigris_cd <- get_tigris_cd("NY")
### Counties
ny_tigris_counties <- get_tigris_counties("NY")
### CBSA
ny_tigris_cbsa <- filter_tigris_cbsa("NY", ny_state, tigris_cbsa)
## Combine
ny_geos <- dplyr::bind_rows(ny_state,
                            ny_tigris_cd, 
                            ny_tigris_counties, 
                            ny_tigris_cbsa)

# Florida
fl_state <- states |> 
  dplyr::filter(STUSPS == "FL") |>
  dplyr::select(NAME) |>
  dplyr::mutate(STATE = "FL",
                UNIT = "State")
## Tigris
### Congressional Districts
fl_tigris_cd <- get_tigris_cd("FL")
### Counties
fl_tigris_counties <- get_tigris_counties("FL")
### CBSA
fl_tigris_cbsa <- filter_tigris_cbsa("FL", fl_state, tigris_cbsa)
## Combine
fl_geos <- dplyr::bind_rows(fl_state,
                            fl_tigris_cd, 
                            fl_tigris_counties, 
                            fl_tigris_cbsa)

# Puerto Rico
pr_state <- states |> 
  dplyr::filter(STUSPS == "PR") |>
  dplyr::select(NAME) |>
  dplyr::mutate(STATE = "PR",
                UNIT = "State")
## Tigris
### Congressional Districts
pr_tigris_cd <- get_tigris_cd("PR")
### Counties
pr_tigris_counties <- get_tigris_counties("PR")
### CBSA
pr_tigris_cbsa <- filter_tigris_cbsa("PR", pr_state, tigris_cbsa)
## Combine
pr_geos <- dplyr::bind_rows(pr_state,
                            pr_tigris_cd, 
                            pr_tigris_counties, 
                            pr_tigris_cbsa)

# ACS Demographic Data

# All
states_acs<- c("NY", "FL", "PR")
geo_units <- c("congressional district", "county", "state")

# All populations except for metro areas
acs_pop_df <- purrr::map(states_acs, function(state) {
  purrr::map(geo_units, 
             get_pop_counts, 
             state = state) |>
    purrr::list_rbind() |>
    dplyr::mutate(STATE = state)
},
.progress = TRUE) |>
  purrr::list_rbind() |>
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
    NAME = gsub(" \\(118th Congress\\)", "", NAME),
    NAME = gsub(", New York|, Florida|, Puerto Rico", "", NAME)
  )
# Populations from metro areas
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

# New York
acs_NY <- dplyr::bind_rows(
  acs_pop_df |> dplyr::filter(STATE == "NY"),
  acs_metro_areas |> dplyr::filter(STATE == "NY")
)

ny_full <- ny_geos |>
  tidylog::left_join(acs_NY, by = c("NAME", "STATE")) |>
  dplyr::mutate(
    PERCENT_HISPANIC_LATINO = scales::percent(
      TOTAL_HISPANIC_LATINO_POPULATION / TOTAL_POPULATION,
      accuracy = 0.01
    )
  ) |>
  dplyr::select(!GEOID)

# Florida
acs_FL <- dplyr::bind_rows(
  acs_pop_df |> dplyr::filter(STATE == "FL"),
  acs_metro_areas |> dplyr::filter(STATE == "FL")
)
fl_full <- fl_geos |>
  tidylog::left_join(acs_FL, by = c("NAME", "STATE")) |>
  dplyr::mutate(
    PERCENT_HISPANIC_LATINO = scales::percent(
      TOTAL_HISPANIC_LATINO_POPULATION / TOTAL_POPULATION,
      accuracy = 0.01
    )
  ) |>
  dplyr::select(!GEOID)

# Puerto Rico
acs_PR <- dplyr::bind_rows(
  acs_pop_df |> dplyr::filter(STATE == "PR"),
  acs_metro_areas |> dplyr::filter(STATE == "PR")
)
pr_full <- pr_geos |>
  tidylog::left_join(acs_PR, by = c("NAME", "STATE")) |>
  dplyr::mutate(
    PERCENT_HISPANIC_LATINO = scales::percent(
      TOTAL_HISPANIC_LATINO_POPULATION / TOTAL_POPULATION,
      accuracy = 0.01
    )
  ) |>
  dplyr::select(!GEOID)


# Save as separate sheets in an xlsx workbook
writexl::write_xlsx(
  list(
    "New York" = ny_full,
    "Florida" = fl_full,
    "Puerto Rico" = pr_full
  ),
  path = "data/congressional_briefing-20250715.xlsx"
)

# Helper functions
get_tigris_cd <- function(state) {
  tigris::congressional_districts(state = state) |>
    dplyr::select(NAMELSAD, geometry) |>
    dplyr::mutate(STATE = state,
                  UNIT = "Congressional District") |>
    dplyr::rename(NAME = NAMELSAD)
}

get_tigris_counties <- function(state) {
  tigris::counties(
    state = state,
    cb = TRUE    # Use cartographic boundaries
  ) |>
    dplyr::select(NAMELSAD, geometry) |>
    dplyr::mutate(STATE = state,
                  UNIT = "County") |>
    dplyr::rename(NAME = NAMELSAD)
}

filter_tigris_cbsa <- function(state, tigris_state, tigris_cbsa) {
  tigris_cbsa |>
    sf::st_filter(tigris_state) |>
    dplyr::filter(grepl("Metro Area", NAMELSAD)) |>
    dplyr::filter(grepl(state, NAMELSAD)) |>
    dplyr::select(NAMELSAD, geometry) |>
    dplyr::rename(NAME = NAMELSAD) |>
    dplyr::mutate(STATE = state, UNIT = "Metro Area")
}

get_pop_counts <- function(geo_unit, state) {
  print(paste("Fetching population counts for", geo_unit, "in", state))
  pop_counts <- tidycensus::get_acs(
    geography = geo_unit,
    variables = c("B03001_001", "B03001_003"),
    state = state,
    survey = "acs5",
    cache_table = TRUE
  )
  return(pop_counts)
}


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