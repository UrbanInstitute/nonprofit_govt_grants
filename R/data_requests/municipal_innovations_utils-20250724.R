#' @title Retrieve Geographic Boundaries for Cities or Counties
#'
#' @description This is a wrapper function that dispatches to either `get_county`
#'   or `get_place` based on the `geo` type specified in the `params` list.
#'   It is designed to fetch geographic boundaries for a city or county
#'   using the `tigris` package.
#'
#' @param params A list containing parameters for geographic lookup.
#'   It must include:
#'   \itemize{
#'     \item `geo`: A character string, either "county" or "place", indicating
#'       whether to retrieve county or incorporated place boundaries.
#'     \item `state`: A two-letter character string for the state abbreviation
#'       (e.g., "PA", "TN").
#'     \item If `geo` is "county":
#'       \itemize{
#'         \item `fips`: A character string or vector of FIPS codes for the
#'           county/counties (e.g., "42101", `c("36005", "36047")`).
#'       }
#'     \item If `geo` is "place":
#'       \itemize{
#'         \item `name`: A character string for the name of the incorporated
#'           place (e.g., "Memphis", "Atlanta").
#'       }
#'   }
#'
#' @return An `sf` (simple features) data frame containing the geographic
#'   boundary/boundaries for the specified entity, as returned by `tigris`.
#'
#' @seealso \code{\link{get_county}}, \code{\link{get_place}}
#' @importFrom dplyr filter
#' @importFrom tigris counties places
#' @export
get_city_geos <- function(params){
  if (params$geo == "county"){
    geo <- get_county(params$fips, params$state)
  } else if (params$geo == "place"){
    geo <- get_place(params$name, params$state)
  }
  return(geo)
}


#' @title Retrieve County Geographic Boundaries
#'
#' @description Fetches geographic boundaries for one or more counties within
#'   a specified state using the `tigris` package.
#'
#' @param county_fips A character string or vector of FIPS codes for the
#'   county/counties (e.g., "42101", `c("36005", "36047", "36061", "36081", "36085")`).
#' @param state A two-letter character string for the state abbreviation
#'   (e.g., "PA", "NY").
#'
#' @return An `sf` (simple features) data frame containing the geographic
#'   boundary/boundaries for the specified county/counties.
#'
#' @importFrom dplyr filter
#' @importFrom tigris counties
#' @export
get_county <- function(county_fips, state){
  counties <- tigris::counties(state = state, cb = TRUE)
  county <- counties |>
    dplyr::filter(GEOID %in% county_fips)
  return(county)
}


#' @title Retrieve Incorporated Place Geographic Boundaries
#'
#' @description Fetches geographic boundaries for an incorporated place (city)
#'   within a specified state using the `tigris` package.
#'
#' @param place_name A character string for the name of the incorporated
#'   place (e.g., "Memphis", "Atlanta").
#' @param state A two-letter character string for the state abbreviation
#'   (e.g., "TN", "GA").
#'
#' @return An `sf` (simple features) data frame containing the geographic
#'   boundary for the specified incorporated place.
#'
#' @importFrom dplyr filter
#' @importFrom tigris places
#' @export
get_place <- function(place_name, state){
  places <- tigris::places(state = state, cb = TRUE)
  place <- places |>
    dplyr::filter(NAME == place_name)
  return(place)
}

#' @title Reformat a Hyphen-Separated String
#'
#' @description This function takes a character string expected to be in the
#'   format "XX-YYY-ZZZ" and reorders its hyphen-separated components to
#'   the "YYY-ZZZ-XX" format.
#'
#' @param input_string A single character string. Expected to have three
#'   components separated by hyphens (e.g., "AA-ART-A00").
#'
#' @return A single character string in the "YYY-ZZZ-XX" format if the
#'   input string is valid and matches the expected structure. Returns `NA`
#'   otherwise.
#'
#' @examples
#' # Valid input:
#' convert_format("AA-ART-A00")
#' # [1] "ART-A00-AA"
#'
#' convert_format("01-EDU-B05")
#' # [1] "EDU-B05-01"
#'
#' # Invalid input (not enough parts):
#' convert_format("AA-ART")
#' # [1] NA
#' # Warning message:
#' # Input string does not match the expected 'XX-YYY-ZZZ' format.
#'
#' # Invalid input (too many parts):
#' convert_format("AA-ART-A00-XYZ")
#' # [1] NA
#' # Warning message:
#' # Input string does not match the expected 'XX-YYY-ZZZ' format.
#'
#' # Invalid input (not a character string):
#' convert_format(123)
#' # [1] NA
#' # Warning message:
#' # Input must be a single character string.
#'
#' @seealso \code{\link{strsplit}}, \code{\link{paste}}
#' @export
convert_format <- function(input_string) {
  # Check if the input is a single character string
  if (!is.character(input_string) || length(input_string) != 1) {
    warning("Input must be a single character string.")
    return(NA)
  }
  
  # Split the string by the hyphen delimiter
  parts <- unlist(strsplit(input_string, "-"))
  
  # Check if the string was successfully split into exactly three parts
  if (length(parts) == 3) {
    # Reconstruct the string in the desired order: YYY-ZZZ-XX
    output_string <- paste(parts[2], parts[3], parts[1], sep = "-")
    return(output_string)
  } else {
    warning("Input string does not match the expected 'XX-YYY-ZZZ' format.")
    return(NA)
  }
}

# Function to summarize city metrics by dynamic grouping columns
#' @title Summarize City Metrics
#' @description This function takes a data frame and a vector of column names
#'   to group by, then calculates various summary statistics for cities.
#' @param data A data frame containing city metrics (e.g., 'cities_metrics').
#' @param grouping_cols A character vector of column names to group the data by.
#'   These columns must exist in the input `data` frame.
#' @return A data frame with summarized metrics, grouped by the specified columns.
#' @examples
#' # Assuming 'cities_metrics' is an existing data frame
#' # with columns like CITY, `Policy Area`, GOVERNMENT_GRANT_DOLLAR_AMOUNT,
#' # PROFIT_MARGIN, and PROFIT_MARGIN_NOGOVTGRANT.
#'
#' # Example 1: Group by CITY and `Policy Area`
#' # summarize_city_metrics(cities_metrics, c("CITY", "Policy Area"))
#'
#' # Example 2: Group by only CITY
#' # summarize_city_metrics(cities_metrics, "CITY")
#'
#' # Example 3: Group by `Policy Area`
#' # summarize_city_metrics(cities_metrics, "`Policy Area`")
summarize_city_metrics <- function(data, grouping_cols) {
  rename_list <- list(
    "Number of 990 filers with government grants" = "num_990filers_govgrants",
    "Total government grants ($)" = "total_govt_grants",
    "Operating surplus with government grants (%)" = "median_profit_margin",
    "Operating surplus without government grants (%)" = "median_profit_margin_no_govt_grants",
    "Share of 990 filers with government grants at risk" = "proportion_at_risk"
  )
  # Check if grouping_cols exist in the data
  if (!all(grouping_cols %in% names(data))) {
    stop("One or more grouping_cols not found in the provided data frame.")
  }
  
  summarized_data <- data |>
    dplyr::group_by(dplyr::across(dplyr::all_of(grouping_cols))) |>
    dplyr::summarise(
      num_990filers_govgrants = dplyr::n(),
      total_govt_grants = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT, na.rm = TRUE),
      median_profit_margin = round(median(PROFIT_MARGIN, na.rm = TRUE), 3),
      median_profit_margin_no_govt_grants = round(median(PROFIT_MARGIN_NOGOVTGRANT, na.rm = TRUE), 3),
      number_at_risk = sum(AT_RISK_NUM, na.rm = TRUE),
      .groups = "drop" # Drop grouping to avoid unexpected behavior in subsequent operations
    ) |>
    dplyr::mutate(
      proportion_at_risk = round(number_at_risk / num_990filers_govgrants, 3)
    ) |>
    dplyr::select(-number_at_risk) |>
    dplyr::mutate(
      num_990filers_govgrants = scales::comma(num_990filers_govgrants),
      total_govt_grants = scales::dollar(total_govt_grants),
      median_profit_margin = scales::percent(median_profit_margin, accuracy = 0.01),
      median_profit_margin_no_govt_grants = scales::percent(median_profit_margin_no_govt_grants, accuracy = 0.01),
      proportion_at_risk = scales::percent(proportion_at_risk, accuracy = 0.01)
    ) |>
    dplyr::rename(!!!rename_list)
  return(summarized_data)
}
