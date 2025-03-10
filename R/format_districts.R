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
                       "th", "st", "nd", "rd", "th", "th", "th", "th", "th", "th")
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

# Example usage:
# For random subset of 3 districts:
# random_districts <- sample(districts, 3)
# format_districts(random_districts)