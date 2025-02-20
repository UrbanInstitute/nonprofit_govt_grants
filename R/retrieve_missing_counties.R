#' Process Missing Counties Data
#' 
#' This function processes a dataset of absent counties, converting state abbreviations
#' to full names and initializing default values for various metrics. It filters the
#' data for a specified state and returns a cleaned dataset.
#'
#' @param all_missing_counties_df A data frame containing county-level data with columns:
#'   \itemize{
#'     \item CENSUS_STATE_ABBR: State abbreviation
#'     \item CENSUS_COUNTY_NAME: County name
#'     \item CENSUS_STATE_NAME: (Optional) Full state name
#'   }
#' @param state Character string. The full name of the state to filter for (e.g., "California")
#'
#' @return A data frame containing processed county data with the following columns:
#'   \itemize{
#'     \item County: County name
#'     \item No. of 990 Filers w/ Gov Grants: Initialized to 0
#'     \item Total Gov Grants ($): Initialized to "0"
#'     \item Operating Surplus (%): Initialized to "0"
#'     \item Operating Surplus w/o Gov Grants (%): Initialized to "0"
#'     \item Share of 990 Filers w/ Gov Grants at Risk: Initialized to "0"
#'   }
#'
#' @import dplyr
#' @import usdata
#'
#' @examples
#' \dontrun{
#' # Process data for California
#' ca_counties <- process_missing_counties(absent_counties_data, "California")
#' 
#' # Process data for New York
#' ny_counties <- process_missing_counties(absent_counties_data, "New York")
#' }
#'
#' @export
retrieve_missing_counties <- function(all_missing_counties_df, state = NA) {
  missing_counties_df <- all_missing_counties_df |>
    dplyr::mutate(
      CENSUS_STATE_NAME = usdata::abbr2state(CENSUS_STATE_ABBR)
    ) |>
    dplyr::mutate(
      CENSUS_STATE_NAME = ifelse(is.na(CENSUS_STATE_NAME), 
                                 "Other US Jurisdictions/Unmapped", 
                                 CENSUS_STATE_NAME),
      `No. of 990 Filers w/ Gov Grants` = "0.00%",
      `Total Gov Grants ($)` = "$0",
      `Operating Surplus (%)` = "0.00%",
      `Operating Surplus w/o Gov Grants (%)` = "0.00%",
      `Share of 990 Filers w/ Gov Grants at Risk` = "0.00%"
    ) |>
    dplyr::select(! CENSUS_STATE_ABBR) |>
    dplyr::rename(County = CENSUS_COUNTY_NAME)
  
  if (!is.na(state)) {
   missing_counties_df <- missing_counties_df |>
     dplyr::filter(CENSUS_STATE_NAME == state) |>
     dplyr::select(! CENSUS_STATE_NAME)
  } else {
    message("No state specified. Returning all missing counties.")
  }
  
  return(missing_counties_df)
}
