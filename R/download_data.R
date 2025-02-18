#' Download Files from URLs
#' @description Downloads files from provided URLs and saves them with specified names
#' 
#' @param file_map named list. Names are output filenames, values are URLs
#' @param path character scalar. Directory path to save files
#' 
#' @return NULL. Downloads files to specified path
#' 
#' @examples
#' # Example with efile data
#' efile_files <- list(
#'   "efile_p01_2021_0225.csv" = "https://nccs-efile.s3.us-east-1.amazonaws.com/public/v2025/F9-P01-T00-SUMMARY-2021.csv",
#'   "efile_p08_2021_0225.csv" = "https://nccs-efile.s3.us-east-1.amazonaws.com/public/v2025/F9-P08-T00-REVENUE-2021.csv"
#' )
#' download_files(efile_files, "data/raw")
#'
#' # Example with BMF data
#' bmf_files <- list(
#'   "unified_bmf.csv" = "https://nccsdata.s3.amazonaws.com/harmonized/bmf/unified/BMF_UNIFIED_V1.1.csv"
#' )
#' download_files(bmf_files, "data/raw")
download_files <- function(file_map, path) {
  
  for(filename in names(file_map)) {
    url <- file_map[[filename]]
    full_path <- file.path(path, filename)
    download.file(url, full_path)
  }
}