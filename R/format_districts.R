format_districts <- function(districts) {
  # Extract numbers from district names
  numbers <- as.numeric(gsub("\\D", "", districts))
  
  # Remove "at Large" districts which will have NA after number extraction
  numbers <- numbers[!is.na(numbers)]
  
  if (length(numbers) < 3) {
    return("")
  } else {
    # Format the numbers with Oxford comma
    if (length(numbers) == 1) {
      return(paste("District", numbers))
    } else {
      formatted <- paste0(
        "and Districts ",
        paste(numbers[-length(numbers)], collapse = ", "),
        ", and ",
        numbers[length(numbers)]
      )
      return(formatted)
    }
  }
}

# Example usage:
# For random subset of 3 districts:
# random_districts <- sample(districts, 3)
# format_districts(random_districts)