#' Download IRS Form 990 E-File Data
#' @description Downloads specified Form 990 E-File parts from NCCS AWS server
#' 
#' @param year numeric scalar. Tax year to download (e.g. 2021)
#' @param date character scalar. Date suffix for saved files (e.g. "0225") 
#' @param path character scalar. Directory path to save files
#' @param parts named list. Form 990 parts to download with part codes as names and file suffixes as values
#' 
#' @return NULL. Downloads files to specified path
#' 
#' @examples
#' parts <- list(
#'   p01 = "F9-P01-T00-SUMMARY",
#'   p08 = "F9-P08-T00-REVENUE"
#' )
#' download_efile(2021, "0225", "data/raw", parts)
download_efile <- function(year, date, path, parts) {
  
  # Base URL 
  base_url <- "https://nccs-efile.s3.us-east-1.amazonaws.com/public/v2025"
  
  # Download each part
  for(p in names(parts)) {
    
    # Construct URLs and filenames
    url <- file.path(base_url, paste0(parts[[p]], "-", year, ".csv"))
    filename <- file.path(path, paste0("efile_", p, "_", year, "_", date, ".csv"))
    
    # Download file
    download.file(url, filename)
  }
}

# Example usage with default parts:
# default_parts <- list(
#   p01 = "F9-P01-T00-SUMMARY",
#   p08 = "F9-P08-T00-REVENUE", 
#   p09 = "F9-P09-T00-EXPENSES",
#   p10 = "F9-P10-T00-BALANCE-SHEET"
# )
# download_efile(2021, "0225", "data/raw", default_parts)