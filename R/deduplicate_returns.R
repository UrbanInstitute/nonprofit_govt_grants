#' Deduplicate Returns by Retaining the Most Recent Filing
#'
#' Identifies EINs with a given return flag (e.g., group or amended), keeps only
#' the most recent return per EIN (by timestamp), and recombines with the
#' non-flagged records.
#'
#' @param data A data.frame of efile records with columns EIN2 and the specified
#'   flag/timestamp columns.
#' @param flag_col Character. Name of the logical column identifying the return
#'   type (e.g., "RETURN_GROUP_X", "RETURN_AMENDED_X").
#' @param timestamp_col Character. Name of the POSIXct column used to determine
#'   the most recent return (e.g., "RETURN_TIME_STAMP").
#'
#' @return A data.frame with duplicates removed, retaining only the most recent
#'   return per EIN for the flagged return type.
deduplicate_returns <- function(data, flag_col, timestamp_col) {
  flagged_eins <- data |>
    dplyr::filter(.data[[flag_col]] == TRUE) |>
    dplyr::pull(EIN2) |>
    unique()

  flagged <- data |>
    dplyr::filter(EIN2 %in% flagged_eins) |>
    dplyr::group_by(EIN2) |>
    dplyr::slice_max(order_by = .data[[timestamp_col]]) |>
    dplyr::ungroup()

  not_flagged <- data |>
    dplyr::filter(!EIN2 %in% flagged_eins)

  dplyr::bind_rows(not_flagged, flagged)
}
