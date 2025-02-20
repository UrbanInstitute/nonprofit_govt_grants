#' Format Percentage Column
#' 
#' Converts a column containing percentage strings (e.g., "10%") to numeric values
#' by removing the % symbol and converting to numeric format.
#' 
#' @param data A data frame containing the percentage column
#' @param col_name The name of the column containing percentage values, in quotes
#' @param new_col_name Optional name for the new formatted column (defaults to "formatted_[original_name]")
#' @return The original data frame with an additional column containing formatted percentages
#' @examples
#' # For a data frame 'df' with column 'Percent_Share'
#' format_percentages(df, "Percent_Share")
#' 
#' # With custom new column name
#' format_percentages(df, "Percent_Share", "clean_percentages")
format_percentages <- function(data, col_name, new_col_name = NULL) {
  # Extract the percentage column
  percentages <- data[[col_name]]
  
  # Remove % symbol and convert to numeric
  formatted_percentages <- as.numeric(gsub("%", "", percentages))
  
  # Set default new column name if none provided
  if (is.null(new_col_name)) {
    new_col_name <- paste0("formatted_", gsub(" ", "_", col_name))
  }
  
  # Add formatted column to data frame
  data[[new_col_name]] <- formatted_percentages
  
  return(data)
}

# Example usage:
# county <- format_percentages(county, "Share of 990 Filers w/ Gov Grants at Risk")