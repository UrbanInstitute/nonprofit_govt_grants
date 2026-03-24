#' Impute Missing Geography via Spatial Join
#'
#' For records where a geographic field is missing (NA or "") but geometry is
#' non-empty, performs a spatial join against a reference sf object, builds a
#' lookup from EIN2 to the matched value, and back-fills the original data.
#'
#' @param data An sf data.frame with columns EIN2, geometry, and `missing_col`.
#' @param missing_col Character. Name of the column to impute (e.g.,
#'   "CENSUS_STATE_ABBR", "CENSUS_COUNTY_NAME").
#' @param reference_sf An sf object to join against (e.g., county or
#'   congressional district boundaries, already in CRS 4326).
#' @param reference_col Character. Name of the column in `reference_sf` that
#'   contains the values to impute. If NULL, defaults to `missing_col`.
#' @param state_fips_lookup Optional named vector mapping state FIPS to state
#'   abbreviations. If provided and `missing_col` is "CENSUS_STATE_ABBR", the
#'   imputed values are translated through this lookup after the spatial join.
#'
#' @return The input `data` with `missing_col` values filled in where possible.
impute_missing_geography <- function(data, missing_col, reference_sf,
                                     reference_col = NULL,
                                     state_fips_lookup = NULL) {
  if (is.null(reference_col)) reference_col <- missing_col

  null_geoms <- data |>
    dplyr::filter(is.na(.data[[missing_col]]) | .data[[missing_col]] == "",
                  !sf::st_is_empty(geometry)) |>
    dplyr::select(EIN2, geometry) |>
    sf::st_as_sf()

  if (nrow(null_geoms) == 0) return(data)

  joined <- sf::st_join(null_geoms, reference_sf, join = sf::st_intersects)

  if (!is.null(state_fips_lookup) && "CENSUS_STATE_FIPS" %in% names(joined)) {
    joined <- joined |>
      dplyr::mutate(imputed_value = state_fips_lookup[CENSUS_STATE_FIPS])
  } else {
    joined <- joined |>
      dplyr::mutate(imputed_value = .data[[reference_col]])
  }

  ein_lookup <- setNames(joined$imputed_value, joined$EIN2)

  data |>
    dplyr::mutate(
      !!missing_col := ifelse(EIN2 %in% names(ein_lookup),
                              ein_lookup[EIN2],
                              .data[[missing_col]])
    )
}
