# ==============================================================================
# Validation: Check processed data integrity before analysis
# ==============================================================================
# Single function that validates the final processed CSV for a given year.
# Called by run_pipeline.R between process and analysis steps.

#' Validate a processed data file
#'
#' Runs a series of checks on the processed CSV and stops with an informative
#' error message if any check fails.
#'
#' @param file_path Character path to the processed CSV file
#' @param year Integer tax year (used in messages)
#' @return TRUE invisibly if all checks pass
validate_processed_data <- function(file_path, year) {

  label <- paste0("[VALIDATION] Year ", year, ": ")

  # (1) File exists
  if (!file.exists(file_path)) {
    stop("[VALIDATION FAILED] Year ", year,
         ": Processed file not found: ", file_path)
  }
  cat(label, "File exists\n")

  df <- data.table::fread(file_path)

  # (2) Expected columns present
  expected_cols <- c(
    "EIN2", "CENSUS_REGION", "CENSUS_COUNTY_NAME", "CENSUS_STATE_NAME",
    "CONGRESS_DISTRICT_NAME", "SUBSECTOR", "EXPENSE_CATEGORY",
    "GOVERNMENT_GRANT_DOLLAR_AMOUNT", "PROFIT_MARGIN",
    "PROFIT_MARGIN_NOGOVTGRANT", "AT_RISK_NUM"
  )
  missing_cols <- setdiff(expected_cols, names(df))
  if (length(missing_cols) > 0) {
    stop("[VALIDATION FAILED] Year ", year,
         ": Missing columns: ", paste(missing_cols, collapse = ", "))
  }
  cat(label, "All ", length(expected_cols), " expected columns present\n")

  # (3) Row count exceeds minimum threshold
  if (nrow(df) < MIN_EXPECTED_ROWS) {
    stop("[VALIDATION FAILED] Year ", year,
         ": Row count ", nrow(df), " is below minimum threshold ",
         MIN_EXPECTED_ROWS)
  }
  cat(label, "Row count ", nrow(df), " >= ", MIN_EXPECTED_ROWS, "\n")

  # (4) No fully-NA critical fields
  critical_fields <- c("EIN2", "CENSUS_STATE_NAME",
                        "GOVERNMENT_GRANT_DOLLAR_AMOUNT", "AT_RISK_NUM")
  for (field in critical_fields) {
    if (all(is.na(df[[field]]))) {
      stop("[VALIDATION FAILED] Year ", year,
           ": Column '", field, "' is entirely NA")
    }
  }
  cat(label, "No fully-NA critical fields\n")

  # (5) AT_RISK_NUM is binary (0/1)
  at_risk_vals <- unique(df$AT_RISK_NUM[!is.na(df$AT_RISK_NUM)])
  if (!all(at_risk_vals %in% c(0, 1))) {
    stop("[VALIDATION FAILED] Year ", year,
         ": AT_RISK_NUM contains non-binary values: ",
         paste(setdiff(at_risk_vals, c(0, 1)), collapse = ", "))
  }
  cat(label, "AT_RISK_NUM is binary\n")

  # (6) GOVERNMENT_GRANT_DOLLAR_AMOUNT — check for non-positive values (informational)
  non_na_grants <- df$GOVERNMENT_GRANT_DOLLAR_AMOUNT[
    !is.na(df$GOVERNMENT_GRANT_DOLLAR_AMOUNT)
  ]
  n_neg <- sum(non_na_grants < 0)
  n_zero <- sum(non_na_grants == 0)
  if (n_neg > 0 || n_zero > 0) {
    cat(label, n_neg, " negative and ", n_zero,
        " zero GOVERNMENT_GRANT_DOLLAR_AMOUNT values",
        " (see 'Notable government grant values' in qa.xlsx)\n")
  } else {
    cat(label, "All government grant amounts > 0\n")
  }

  # (7) CENSUS_STATE_NAME values are recognized
  valid_states <- c(STATE_NAMES, "Other/unmapped jurisdictions")
  actual_states <- unique(df$CENSUS_STATE_NAME[!is.na(df$CENSUS_STATE_NAME)])
  unrecognized <- setdiff(actual_states, valid_states)
  if (length(unrecognized) > 0) {
    stop("[VALIDATION FAILED] Year ", year,
         ": Unrecognized CENSUS_STATE_NAME values: ",
         paste(unrecognized, collapse = ", "))
  }
  cat(label, "All state names recognized\n")

  cat(label, "ALL CHECKS PASSED\n")
  invisible(TRUE)
}
