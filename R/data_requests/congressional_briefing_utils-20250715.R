state_geos <- function(state_abbr, state_geos, cbsa_geos) {
  state <- state_geos |>
    dplyr::filter(STUSPS == state_abbr) |>
    dplyr::select(NAME) |>
    dplyr::mutate(STATE = state_abbr, UNIT = "State")
  state_cd <- get_tigris_cd(state_abbr)
  state_counties <- get_tigris_counties(state_abbr)
  state_cbsa <- filter_tigris_cbsa(state_abbr, state, cbsa_geos)
  geo_df <- dplyr::bind_rows(state, state_cd, state_counties, state_cbsa)
  return(geo_df)
}

get_tigris_cd <- function(state) {
  tigris::congressional_districts(state = state) |>
    dplyr::select(NAMELSAD, geometry) |>
    dplyr::mutate(STATE = state, UNIT = "Congressional District") |>
    dplyr::rename(NAME = NAMELSAD)
}

get_tigris_counties <- function(state) {
  tigris::counties(
    state = state,
    cb = TRUE    # Use cartographic boundaries
  ) |>
    dplyr::select(NAMELSAD, geometry) |>
    dplyr::mutate(STATE = state, UNIT = "County") |>
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

get_state_demographics <- function(acs_state_pops, acs_metro_area_pops, tigris_state_geo, state_abbr){
  acs_state_full <- dplyr::bind_rows(
    acs_state_pops |> dplyr::filter(STATE == state_abbr),
    acs_metro_area_pops |> dplyr::filter(STATE == state_abbr)
  )
  
  state_demo <- tigris_state_geo |>
    tidylog::left_join(acs_state_full, by = c("NAME", "STATE")) |>
    dplyr::mutate(
      PERCENT_HISPANIC_LATINO = round(
        TOTAL_HISPANIC_LATINO_POPULATION / TOTAL_POPULATION,
        2
      ) * 100,
    ) |>
    dplyr::arrange(UNIT, desc(PERCENT_HISPANIC_LATINO)) |>
    dplyr::select(!GEOID) |>
    sf::st_drop_geometry()
  return(state_demo)
}

summarise_state_data <- function(full_metrics, state){
  state_metrics <- full_metrics |>
    dplyr::filter(CENSUS_STATE_NAME == state) |>
    tidyr::pivot_longer(
      cols = c(CENSUS_COUNTY_NAME, CONGRESS_DISTRICT_NAME, CBSA_NAME, CENSUS_STATE_NAME),
      names_to = "UNIT",
      values_to = "NAME"
    ) |>
    dplyr::group_by(NAME) |>
    dplyr::summarise(
      num_990filers_govgrants = dplyr::n(),
      total_govt_grants = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT, na.rm = TRUE),
      median_profit_margin = median(PROFIT_MARGIN, na.rm = TRUE),
      median_profit_margin_no_govt_grants = median(PROFIT_MARGIN_NOGOVTGRANT, na.rm = TRUE),
      number_at_risk = sum(AT_RISK_NUM, na.rm = TRUE)
    )
  return(state_metrics)
}