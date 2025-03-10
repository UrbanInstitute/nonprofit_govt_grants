#' @title Summarize Nonprofit Financial Data
#' 
#' @description This function summarizes nonprofit financial data by calculating the number of 990 filers with government grants, the total amount of government grants, the median profit margin, the median profit margin without government grants, and the number of organizations at risk.
#' 
#' @param data The input dataframe
#' @param group_var Optional grouping variable name as string (e.g., "CENSUS_STATE_NAME")
#' @param group_var_rename Optional new name for the grouping variable (e.g., "State")
#' @param qa Logical indicating whether to perform quality assurance summarization
#' @param format Logical indicating whether to format the output numbers with percentages and dollar signs
#' 
#' @return A dataframe with summarized nonprofit financial metrics
summarize_nonprofit_data <- function(data,
                                     group_var = NULL,
                                     group_var_rename = NULL,
                                     qa = FALSE,
                                     format = TRUE) {
  # Start the pipeline
  summary <- data
  
  # Add grouping if specified
  if (qa) {
    summary <- summary |>
      dplyr::group_by(CENSUS_STATE_NAME, .data[[group_var]])
  } else if (!is.null(group_var)) {
    summary <- summary |>
      dplyr::group_by(.data[[group_var]])
  }
  
  # Perform summarization
  summary <- summary |>
    dplyr::summarise(
      num_990filers_govgrants = dplyr::n(),
      total_govt_grants = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT, na.rm = TRUE),
      median_profit_margin = median(PROFIT_MARGIN, na.rm = TRUE),
      median_profit_margin_no_govt_grants = median(PROFIT_MARGIN_NOGOVTGRANT, na.rm = TRUE),
      number_at_risk = sum(AT_RISK_NUM, na.rm = TRUE)
    ) |>
    dplyr::mutate(proportion_at_risk = number_at_risk / num_990filers_govgrants)
  
  # Select columns
  if (qa) {
    summary <- summary |>
      dplyr::select(
        CENSUS_STATE_NAME,!!sym(group_var),
        num_990filers_govgrants,
        total_govt_grants,
        median_profit_margin,
        median_profit_margin_no_govt_grants,
        proportion_at_risk
      )
  } else if (!is.null(group_var)) {
    summary <- summary |>
      dplyr::select(
        !!sym(group_var),
        num_990filers_govgrants,
        total_govt_grants,
        median_profit_margin,
        median_profit_margin_no_govt_grants,
        proportion_at_risk
      )
  } else {
    summary <- summary |>
      dplyr::select(
        num_990filers_govgrants,
        total_govt_grants,
        median_profit_margin,
        median_profit_margin_no_govt_grants,
        proportion_at_risk
      )
  }
  
  # Format numbers
  if (format == TRUE) {
    summary <- summary |>
      dplyr::mutate(
        num_990filers_govgrants = scales::comma(num_990filers_govgrants),
        total_govt_grants = scales::dollar(total_govt_grants),
        median_profit_margin = scales::percent(median_profit_margin, accuracy = 0.01),
        median_profit_margin_no_govt_grants = scales::percent(median_profit_margin_no_govt_grants, accuracy = 0.01),
        proportion_at_risk = scales::percent(proportion_at_risk, accuracy = 0.01)
      )
  }
  
  # Rename columns
  rename_list <- list(
    "Number of 990 filers with government grants" = "num_990filers_govgrants",
    "Total government grants ($)" = "total_govt_grants",
    "Operating surplus with government grants (%)" = "median_profit_margin",
    "Operating surplus without government grants (%)" = "median_profit_margin_no_govt_grants",
    "Share of 990 filers with government grants at risk" = "proportion_at_risk"
  )
  
  # Add group variable rename if provided
  if (!is.null(group_var) && !is.null(group_var_rename)) {
    rename_list[[group_var_rename]] <- group_var
  }
  
  summary <- summary |>
    dplyr::rename(!!!rename_list)
  
  return(summary)
}
