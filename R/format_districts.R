#' Format Congressional District Names Into a Readable List
#'
#' @description
#' Converts a vector of Congressional district names into a readable text string
#' with proper ordinal formatting and Oxford comma. The function extracts district
#' numbers from the input strings, removes "at Large" districts (which have no numbers),
#' and formats the result with proper ordinals and comma placement.
#'
#' @param districts A character vector of Congressional district names (e.g., "District 1", "NY-12")
#'
#' @return A character string with properly formatted district numbers in a readable list.
#'   If no numbered districts remain (after removing "at Large" districts),
#'   an empty string is returned. For 1-2 districts, returns a simplified format.
#'   For 3+ districts, returns a string in the format
#'   "and the 1st, 2nd, and 3rd congressional districts".
#'
#' @details
#' The function handles ordinal suffixes correctly (1st, 2nd, 3rd, 4th, etc.) and
#' accounts for special cases like 11th, 12th, and 13th. Districts without extractable
#' numbers (like "at Large" districts) are excluded from the final output.
#'
#' @examples
#' districts <- c("District 1", "NY-12", "FL-07")
#' format_districts(districts)
#' # Returns: "and the 1st, 7th, and 12th congressional districts"
#'
#' # With fewer than 3 valid districts
#' format_districts(c("District at Large", "District 2"))
#' # Returns: "and the 2nd congressional district"
#'
#' format_districts(c("District at Large"))
#' # Returns: ""
#'
#' @export
format_districts <- function(districts) {
  # Extract numbers from district names
  numbers <- as.numeric(gsub("\\D", "", districts))

  # Remove "at Large" districts which will have NA after number extraction
  numbers <- numbers[!is.na(numbers)]

  # Helper: convert number to ordinal string (1st, 2nd, 3rd, 10th, 11th, etc.)
  make_ordinal <- function(n) {
    last_two <- n %% 100
    if (last_two %in% c(11, 12, 13)) return(paste0(n, "th"))
    suffix <- switch(n %% 10 + 1L,
                     "th", "st", "nd", "rd", "th", "th", "th", "th", "th", "th")
    paste0(n, suffix)
  }

  if (length(numbers) == 0) {
    return("")
  } else if (length(numbers) == 1) {
    result <- make_ordinal(numbers)
    result <- paste("and the", result, "congressional district")
    return(result)
  } else if (length(numbers) == 2) {
    ordinals <- sapply(numbers, make_ordinal)
    result <- paste(ordinals[1], "and", ordinals[2])
    result <- paste("and the", result, "congressional districts")
    return(result)
  } else {
    # Convert numbers to ordinals
    ordinals <- sapply(numbers, make_ordinal)

    # Combine with Oxford comma and "and"
    result <- paste(
      paste(ordinals[-length(ordinals)], collapse = ", "),
      ordinals[length(ordinals)],
      sep = ", and "
    )

    # Add the final text
    result <- paste("and the", result, "congressional districts")
    return(result)
  }
}
