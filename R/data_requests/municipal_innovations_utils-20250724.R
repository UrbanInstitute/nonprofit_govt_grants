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
  if (length(grouping_cols) == 0){
    summarized_data <- data
  } else{
    summarized_data <- data |>
      dplyr::group_by(dplyr::across(dplyr::all_of(grouping_cols)))
  }
  summarized_data <- summarized_data |>
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

convert_ntee_to_v2 <- function(ntee_irs) {
  num_chars <- nchar(ntee_irs)
  if (num_chars < 3) {
    return(NA)
  }
  else {
    first_char <- substr(ntee_irs, 1, 1) |> toupper()
    industry.group <- classify_industry_group(ntee_irs)
    levels <- recode_ntee_levels(ntee_irs)
    if (all(is.na(levels))){
      return(NA)
    } else {
      return(paste0(industry.group, "-", first_char, levels$Level3, levels$Level4, "-", levels$Level5))
    }
  }
}

classify_industry_group <- function(ntee_code) {
  #' Classifies an NTEE code into a broader category based on predefined rules.
  #'
  #' This function prioritizes specific codes for Universities and Hospitals,
  #' then applies broader category rules based on the NTEE code's first letter.
  #'
  #' @param ntee_code The NTEE code as a string (e.g., "A12", "B40", "E21").
  #'                  The function is case-insensitive for the NTEE code.
  #'
  #' @return A 3-letter classification code (e.g., "ART", "UNI", "HOS").
  #'         Returns "UNU" (Unknown, Unclassified) if the code does not match
  #'         any known categories or if the input is not a valid string.
  #'
  #' @examples
  #' classify_ntee_code("A12") # Returns "ART"
  #' classify_ntee_code("B40") # Returns "UNI"
  #' classify_ntee_code("E21") # Returns "HOS"
  #' classify_ntee_code("Z99") # Returns "UNU"
  
  if (is.null(ntee_code) || !is.character(ntee_code) || nchar(ntee_code) == 0) {
    return("UNU")
  }
  
  ntee_code_upper <- toupper(ntee_code)
  
  # --- Step 1: Check for specific NTEE codes (Universities and Hospitals) ---
  # These are exceptions to their broader 'B' (Education) and 'E' (Health) categories.
  
  # Universities (UNI)
  university_codes <- c("B40", "B41", "B42", "B43", "B50")
  if (ntee_code_upper %in% university_codes) {
    return("UNI")
  }
  
  # Hospitals (HOS)
  hospital_codes <- c("E20", "E21", "E22", "E24")
  if (ntee_code_upper %in% hospital_codes) {
    return("HOS")
  }
  
  # --- Step 2: Classify based on the first letter of the NTEE code ---
  first_char <- substr(ntee_code_upper, 1, 1)
  
  if (first_char == 'A') {
    return("ART")  # Arts, Culture, and Humanities
  } else if (first_char == 'B') {
    # 'B' codes that are not specific university codes fall into EDU
    return("EDU")  # Education (excluding universities)
  } else if (first_char %in% c('C', 'D')) {
    return("ENV")  # Environment and Animals
  } else if (first_char %in% c('E', 'F', 'G', 'H')) {
    # 'E', 'F', 'G', 'H' codes that are not specific hospital codes fall into HEL
    return("HEL")  # Health (excluding hospitals)
  } else if (first_char %in% c('I', 'J', 'K', 'L', 'M', 'N', 'O', 'P')) {
    return("HMS")  # Human Services
  } else if (first_char == 'Q') {
    return("IFA")  # International, Foreign Affairs
  } else if (first_char %in% c('R', 'S', 'T', 'U', 'V', 'W')) {
    return("PSB")  # Public, Societal Benefit
  } else if (first_char == 'X') {
    return("REL")  # Religion Related
  } else if (first_char == 'Y') {
    return("MMB")  # Mutual/Membership Benefit
  } else if (first_char == 'Z') {
    return("UNU")  # Unknown, Unclassified
  } else {
    # Catch-all for any other characters not matching the rules
    return("UNU")
  }
}

recode_ntee_levels <- function(ntee_code_str) {
  #' Recodes an NTEE code into Level 3 (Division), Level 4 (Subdivision),
  #' and Level 5 (Organizational Type) based on specific rules.
  #'
  #' This function processes a given NTEE code string to extract relevant
  #' digits and apply a series of conditional rules to classify it into
  #' different hierarchical levels.
  #'
  #' @param ntee_code_str The NTEE code as a string (e.g., "A32", "A02", "A1132").
  #'
  #' @return A list containing the calculated Level 3, Level 4, and Level 5 values,
  #'         or NA for levels if the input is invalid or rules cannot be applied.
  #'         The list components are named 'Level3', 'Level4', and 'Level5'.
  #'
  #' @examples
  #' recode_ntee_levels("A32")
  #' recode_ntee_levels("A02")
  #' recode_ntee_levels("A1132")
  #' recode_ntee_levels("B25")
  #' recode_ntee_levels("C01")
  #' recode_ntee_levels("XYZ") # Invalid code example
  #' recode_ntee_levels(NA)    # NA input
  #' recode_ntee_levels("")    # Empty string input
  
  # Initialize results
  level3_val <- NA
  level4_val <- NA
  level5_val <- NA
  
  # --- Input Validation ---
  if (is.null(ntee_code_str) || !is.character(ntee_code_str) || nchar(ntee_code_str) == 0) {
    message("Invalid NTEE code input: must be a non-empty string.")
    return(list(Level3 = NA, Level4 = NA, Level5 = NA))
  }
  
  ntee_code_upper <- toupper(ntee_code_str)
  code_length <- nchar(ntee_code_upper)
  
  # --- Intermediary Step: Define digits23 and digits45 ---
  
  # digits23: 2nd and 3rd characters
  if (code_length >= 3) {
    digits23_str <- substr(ntee_code_upper, 2, 3)
    if (digits23_str %in% c("6A", "6B", "6C", "6E", "6A", "9B", "9B", "2A", "2B", "2C", 
                            "6A", "6B", "6C", "6D", "6E", "6F", "4A", "4B", "2A", "2B", "6A", 
                            "7A")){
      level3_val <- substr(digits23_str, 1, 1)
      level4_val <- substr(digits23_str, 2, 2)
      return(list(Level3 = level3_val, Level4 = level4_val, Level5 = "RG"))
    }
    digits23_num <- suppressWarnings(as.numeric(digits23_str)) # Convert to numeric, suppress warnings for non-numeric
    if (is.na(digits23_num)) {
      digits23_str <- paste0("0", substr(ntee_code_upper, 2, 2))
      digits23_num <- suppressWarnings(as.numeric(digits23_str))
      message("Invalid digits for NTEE code: ", ntee_code_str, ". Digits 2-3 are not numeric. Converted to: ", digits23_str)
      if (is.na(digits23_num)){
        return(list(Level3 = NA, Level4 = NA, Level5 = NA))
      }
    }
  } else {
    # If code is too short, digits23 cannot be extracted, return NA
    message("NTEE code too short to extract digits23: ", ntee_code_str)
    return(list(Level3 = NA, Level4 = NA, Level5 = NA))
  }
  
  # digits45: 4th and 5th characters, default to "00" if not present
  if (code_length >= 5) {
    digits45_str <- substr(ntee_code_upper, 4, 5)
  } else {
    digits45_str <- "00"
  }
  digits45_num <- suppressWarnings(as.numeric(digits45_str))
  if (is.na(digits45_num)) {
    message("Invalid digits for NTEE code: ", ntee_code_str, ". Digits 4-5 are not numeric.")
    digits45_str <- "00"
  }
  
  
  # --- Level 3 - Division ---
  # If digits23 > 19, then Level 3 is the first digit of digits23.
  # If digits23 < 20, then Level 3 is the first digit of digits45.
  if (digits23_num > 19) {
    level3_val <- as.numeric(substr(digits23_str, 1, 1))
  } else if (digits23_num < 20) {
    level3_val <- as.numeric(substr(digits45_str, 1, 1))
  }
  
  # --- Level 4 - Subdivision ---
  # If digits23 > 19, then Level 4 is the second digit of digits23.
  # If digits23 < 20, then Level 4 is the second digit of digits45.
  # Note: Assuming "Level 3" in the prompt for Level 4 rules was a typo and should be "Level 4".
  if (digits23_num > 19) {
    level4_val <- as.numeric(substr(digits23_str, 2, 2))
  } else if (digits23_num < 20) {
    level4_val <- as.numeric(substr(digits45_str, 2, 2))
  }
  
  # --- Level 5 - Organizational Type ---
  # Based on digits23
  if (digits23_num >= 20) {
    level5_val <- "RG"
  } else if (digits23_num == 1) { # Note: This is 01, not 1. Corrected from 01 to 1 for numeric comparison
    level5_val <- "AA"
  } else if (digits23_num == 2) { # Corrected from 02 to 2
    level5_val <- "MT"
  } else if (digits23_num == 3) { # Corrected from 03 to 3
    level5_val <- "PS"
  } else if (digits23_num == 5) { # Corrected from 05 to 5
    level5_val <- "RP"
  } else if (digits23_num == 11) {
    level5_val <- "MS"
  } else if (digits23_num == 12) {
    level5_val <- "MM"
  } else if (digits23_num == 19) {
    level5_val <- "NS"
  } else {
    level5_val <- NA # No specific rule for this digits23
  }
  
  return(list(Level3 = level3_val, Level4 = level4_val, Level5 = level5_val))
}