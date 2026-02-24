#' Download Files from URLs
#' @description Downloads files from provided URLs and saves them with specified names.
#'   Skips files that already exist (unless overwrite = TRUE) and reports progress.
#'
#' @param file_map named list. Names are output filenames, values are URLs
#' @param path character scalar. Directory path to save files
#' @param overwrite logical. If FALSE (default), skip files that already exist
#' @param timeout integer. Download timeout in seconds (default 600 for large files)
#'
#' @return NULL (invisible). Downloads files to specified path.
#'
#' @examples
#' efile_files <- list(
#'   "efile_p01_2021_0225.csv" = "https://nccs-efile.s3.us-east-1.amazonaws.com/public/v2025/F9-P01-T00-SUMMARY-2021.csv",
#'   "efile_p08_2021_0225.csv" = "https://nccs-efile.s3.us-east-1.amazonaws.com/public/v2025/F9-P08-T00-REVENUE-2021.csv"
#' )
#' download_files(efile_files, "data/raw")
download_files <- function(file_map, path, overwrite = FALSE, timeout = 600) {
  old_timeout <- getOption("timeout")
  on.exit(options(timeout = old_timeout))
  options(timeout = timeout)

  for (filename in names(file_map)) {
    url <- file_map[[filename]]
    full_path <- file.path(path, filename)

    if (!overwrite && file.exists(full_path)) {
      message("Skipping ", filename, " (already exists)")
      next
    }

    message("Downloading ", filename, " ...")
    tryCatch(
      {
        download.file(url, full_path)
        message("  Done: ", filename)
      },
      error = function(e) {
        warning("Failed to download ", filename, ": ", conditionMessage(e))
      }
    )
  }

  invisible(NULL)
}
