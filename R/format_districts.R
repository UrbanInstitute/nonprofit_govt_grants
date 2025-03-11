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
#'   If fewer than 3 districts are provided (after removing "at Large" districts),
#'   an empty string is returned. Otherwise, returns a string in the format
#'   "and 1st, 2nd, and 3rd Congressional districts".
#'
#' @details
#' The function handles ordinal suffixes correctly (1st, 2nd, 3rd, 4th, etc.) and
#' accounts for special cases like 11th, 12th, and 13th. Districts without extractable
#' numbers (like "at Large" districts) are excluded from the final output.
#'
#' @examples
#' districts <- c("District 1", "NY-12", "FL-07")
#' format_districts(districts)
#' # Returns: "and 1st, 7th, and 12th Congressional districts"
#'
#' # With fewer than 3 valid districts
#' format_districts(c("District at Large", "District 2"))
#' # Returns: ""
#'
#' @export
format_districts <- function(districts) {
  # Extract numbers from district names
  numbers <- as.numeric(gsub("\\D", "", districts))
  
  # Remove "at Large" districts which will have NA after number extraction
  numbers <- numbers[!is.na(numbers)]
  
  if (length(numbers) < 3) {
    return("")
  } else {
    # Format the numbers with Oxford comma
    make_ordinal <- function(n) {
      if (n %in% c(11,12,13)) return(paste0(n, "th"))
      suffix <- switch(n %% 10,
                       "st", "nd", "rd", "th", "th", "th", "th", "th", "th", "th")
      paste0(n, suffix)
    }
    
    # Convert numbers to ordinals
    ordinals <- sapply(numbers, make_ordinal)
    
    # Combine with Oxford comma and "and"
    result <- paste(
      paste(ordinals[-length(ordinals)], collapse = ", "),
      ordinals[length(ordinals)],
      sep = ", and "
    )
    
    # Add the final text
    result <- paste("and", result, "Congressional districts")
    return(result)
  }
}
