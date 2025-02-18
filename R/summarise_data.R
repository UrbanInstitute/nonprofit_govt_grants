#' @title Function to create tables used in fact sheets
#' @description This function creates a data.frame by geography (national, regional
#' and state), disaggregated by either subsector or expense category for the 
#' following 5 metrics: (1) mean and median months of cash on hand with tangible
#' assets included, (2) mean and median months of cash on hand without tangible
#' assets included, (3) total government grants received, (4) proportion of
#' nonprofits receiving X% of their revenue from government grants, and (5) total
#' number of nonprofits in the sample.
#' @param data data.frame object of the procedded data
#' @param geo_var character vector of the geography variable
#' @param geo character vector of the geography to filter the data
#' @param group_var character vector of the metric to disaggregate the data
#' @param national logical scalar to indicate if the geographic scope is national
#' @return data.frame
summarise_data <- function(data, geo_var, geo, group_var, national = FALSE) {
  if (national) {
    factsheet_df <- data |>
      dplyr::filter(TANGIBLE_ASSETS_REPORTED != "")
  } else {
    factsheet_df <- data |>
      dplyr::filter(!!sym(geo_var) == geo,
                    TANGIBLE_ASSETS_REPORTED != "")
  }
  factsheet_df <- factsheet_df |>
    dplyr::group_by(!!sym(group_var)) |> # parameter 2 - aggregation
    dplyr::summarise(
      mean_months_cash_on_hand_tangibleassets = mean(
        ifelse(TANGIBLE_ASSETS_REPORTED == "Y", MONTHS_CASH_ON_HAND, NA),
        na.rm = TRUE
      ),
      median_months_cash_on_hand_tangibleassets = median(
        ifelse(TANGIBLE_ASSETS_REPORTED == "Y", MONTHS_CASH_ON_HAND, NA),
        na.rm = TRUE
      ),
      mean_months_cash_on_hand_notangibleassets = mean(
        ifelse(TANGIBLE_ASSETS_REPORTED == "N", MONTHS_CASH_ON_HAND, NA),
        na.rm = TRUE
      ),
      median_months_cash_on_hand_notangibleassets = median(
        ifelse(TANGIBLE_ASSETS_REPORTED == "N", MONTHS_CASH_ON_HAND, NA),
        na.rm = TRUE
      ),
      proportion_govt_grants_20 = dplyr::n_distinct(
        ifelse(
          PROPORTION_GOVT_GRANT > 0 &
            PROPORTION_GOVT_GRANT <= 0.2,
          EIN2,
          NA
        )
      ),
      proportion_govt_grants_40 = dplyr::n_distinct(
        ifelse(
          PROPORTION_GOVT_GRANT > 0.2 &
            PROPORTION_GOVT_GRANT <= 0.4,
          EIN2,
          NA
        )
      ),
      proportion_govt_grants_60 = dplyr::n_distinct(
        ifelse(
          PROPORTION_GOVT_GRANT > 0.4 &
            PROPORTION_GOVT_GRANT <= 0.6,
          EIN2,
          NA
        )
      ),
      proportion_govt_grants_80 = dplyr::n_distinct(
        ifelse(
          PROPORTION_GOVT_GRANT > 0.6 &
            PROPORTION_GOVT_GRANT <= 0.8,
          EIN2,
          NA
        )
      ),
      proportion_govt_grants_100 = dplyr::n_distinct(
        ifelse(PROPORTION_GOVT_GRANT > 0.8 &
                 PROPORTION_GOVT_GRANT <= 1, EIN2, NA)
      ),
      total_govt_grants = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT, na.rm = TRUE),
      total_number_nonprofits = dplyr::n_distinct(EIN2)
    ) |>
    dplyr::mutate(
      dplyr::across(
        proportion_govt_grants_20:proportion_govt_grants_100,
        ~ .x / total_number_nonprofits
      )
    ) |>
    dplyr::mutate(
      dplyr::across(
        proportion_govt_grants_20:proportion_govt_grants_100,
        scales::percent
      ),
      total_govt_grants = scales::dollar(total_govt_grants),
      dplyr::across(
        mean_months_cash_on_hand_tangibleassets:median_months_cash_on_hand_notangibleassets,
        scales::comma,
        2
      )
    ) 
    dplyr::select(
      !!sym(group_var),
      mean_months_cash_on_hand_tangibleassets,
      median_months_cash_on_hand_tangibleassets,
      mean_months_cash_on_hand_notangibleassets,
      median_months_cash_on_hand_notangibleassets,
      median_days_cash_on_hand,
      median_profit_margin,
      total_govt_grants,
      proportion_govt_grants_20,
      proportion_govt_grants_40,
      proportion_govt_grants_60,
      proportion_govt_grants_80,
      proportion_govt_grants_100,
      total_number_nonprofits
    )
  return(factsheet_df)
}

summarize_by_region <- function(group_var) {
  # Create the grouped summary
  region_summary <- full_sample_proc |>
    dplyr::filter(CENSUS_STATE_ABBR == "MA") |>
    dplyr::group_by(.data[[group_var]]) |>
    dplyr::summarise(
      number_reporting_govt_grants = dplyr::n(),
      total_govt_grants = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT, na.rm = TRUE),
      median_profit_margin = median(PROFIT_MARGIN, na.rm = TRUE),
      median_profit_margin_no_govt_grants = median(PROFIT_MARGIN_NOGOVTGRANT, na.rm = TRUE),
      median_days_cash_on_hand = median(DAYS_CASH_ON_HAND, na.rm = TRUE),
      lessthan30days_cash_on_hand = sum(DAYS_CASH_ON_HAND < 30, na.rm = TRUE),
      btwn30and90days_cash_on_hand = sum(DAYS_CASH_ON_HAND >= 30 & DAYS_CASH_ON_HAND < 90, na.rm = TRUE),
      morethan90days_cash_on_hand = sum(DAYS_CASH_ON_HAND >= 90, na.rm = TRUE),
      median_months_cash_on_hand = median(MONTHS_CASH_ON_HAND, na.rm = TRUE),
      lessthan1month_cash_on_hand = sum(MONTHS_CASH_ON_HAND < 1, na.rm = TRUE),
      btwn1and3months_cash_on_hand = sum(MONTHS_CASH_ON_HAND >= 1 & MONTHS_CASH_ON_HAND < 3, na.rm = TRUE),
      morethan3months_cash_on_hand = sum(MONTHS_CASH_ON_HAND >= 3, na.rm = TRUE)
    )
  
  # Get number of nonprofits
  numnonprofits_region <- bmf_sample |>
    dplyr::filter(CENSUS_STATE_ABBR == "MA",
                  ORG_YEAR_LAST >= 2021) |>
    dplyr::group_by(.data[[group_var]]) |>
    dplyr::summarise(num_nonprofits = dplyr::n()) |>
    sf::st_drop_geometry()
  
  # Join the datasets
  region_summary <- tidylog::left_join(region_summary, 
                                       numnonprofits_region, 
                                       by = group_var)
  
  # Format the numbers
  region_summary <- region_summary |>
    dplyr::mutate(
      total_govt_grants = scales::dollar(total_govt_grants),
      median_profit_margin = scales::percent(median_profit_margin, accuracy = 0.01),
      median_profit_margin_no_govt_grants = scales::percent(median_profit_margin_no_govt_grants,
                                                            accuracy = 0.01),
      median_days_cash_on_hand = scales::number(median_days_cash_on_hand),
      median_months_cash_on_hand = scales::number(median_months_cash_on_hand),
      lessthan30days_cash_on_hand = scales::percent(lessthan30days_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01),
      btwn30and90days_cash_on_hand = scales::percent(btwn30and90days_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01),
      morethan90days_cash_on_hand = scales::percent(morethan90days_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01),
      lessthan1month_cash_on_hand = scales::percent(lessthan1month_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01),
      btwn1and3months_cash_on_hand = scales::percent(btwn1and3months_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01),
      morethan3months_cash_on_hand = scales::percent(morethan3months_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01)
    ) |> 
    dplyr::select(
      !!sym(group_var),
      num_nonprofits,
      number_reporting_govt_grants,
      total_govt_grants,
      median_profit_margin,
      median_profit_margin_no_govt_grants,
      median_days_cash_on_hand,
      lessthan30days_cash_on_hand,
      btwn30and90days_cash_on_hand,
      morethan90days_cash_on_hand,
      median_months_cash_on_hand,
      lessthan1month_cash_on_hand,
      btwn1and3months_cash_on_hand,
      morethan3months_cash_on_hand
    ) |>
    dplyr::rename(
      "Region Name" = !!sym(group_var),
      "Total Number of Nonprofits" = num_nonprofits,
      "Number of 990 Filers reporting Government Grants" = number_reporting_govt_grants,
      "Total Government Grants ($USD)" = total_govt_grants,
      "Median Profit Margin (%)" = median_profit_margin,
      "Median Profit Margin (Without Government Grants) (%)" = median_profit_margin_no_govt_grants,
      "Median Days Cash on Hand" = median_days_cash_on_hand,
      "% With Less than 30 Days Cash on Hand" = lessthan30days_cash_on_hand,
      "% With Between 30 and 90 Days Cash on Hand" = btwn30and90days_cash_on_hand,
      "% With More than 90 Days Cash on Hand" = morethan90days_cash_on_hand,
      "Median Months Cash on Hand" = median_months_cash_on_hand,
      "% With Less than 1 Month Cash on Hand" = lessthan1month_cash_on_hand,
      "% With Between 1 and 3 Months Cash on Hand" = btwn1and3months_cash_on_hand,
      "% With More than 3 Months Cash on Hand" = morethan3months_cash_on_hand
    )
  
  return(region_summary)
}

summarize_state <- function() {
  # Create the state summary
  state_summary <- full_sample_proc |>
    dplyr::filter(CENSUS_STATE_ABBR == "MA") |>
    dplyr::summarise(
      number_reporting_govt_grants = dplyr::n(),
      total_govt_grants = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT, na.rm = TRUE),
      median_profit_margin = median(PROFIT_MARGIN, na.rm = TRUE),
      median_profit_margin_no_govt_grants = median(PROFIT_MARGIN_NOGOVTGRANT, na.rm = TRUE),
      median_days_cash_on_hand = median(DAYS_CASH_ON_HAND, na.rm = TRUE),
      lessthan30days_cash_on_hand = sum(DAYS_CASH_ON_HAND < 30, na.rm = TRUE),
      btwn30and90days_cash_on_hand = sum(DAYS_CASH_ON_HAND >= 30 & DAYS_CASH_ON_HAND < 90, na.rm = TRUE),
      morethan90days_cash_on_hand = sum(DAYS_CASH_ON_HAND >= 90, na.rm = TRUE),
      median_months_cash_on_hand = median(MONTHS_CASH_ON_HAND, na.rm = TRUE),
      lessthan1month_cash_on_hand = sum(MONTHS_CASH_ON_HAND < 1, na.rm = TRUE),
      btwn1and3months_cash_on_hand = sum(MONTHS_CASH_ON_HAND >= 1 & MONTHS_CASH_ON_HAND < 3, na.rm = TRUE),
      morethan3months_cash_on_hand = sum(MONTHS_CASH_ON_HAND >= 3, na.rm = TRUE)
    )
  
  # Get total number of nonprofits
  total_nonprofits <- bmf_sample |>
    dplyr::filter(CENSUS_STATE_ABBR == "MA",
                  ORG_YEAR_LAST >= 2021) |>
    dplyr::summarise(num_nonprofits = dplyr::n()) |>
    sf::st_drop_geometry()
  
  # Add total nonprofits to state summary
  state_summary$num_nonprofits <- total_nonprofits$num_nonprofits
  
  # Format the numbers
  state_summary <- state_summary |>
    dplyr::mutate(
      total_govt_grants = scales::dollar(total_govt_grants),
      median_profit_margin = scales::percent(median_profit_margin, accuracy = 0.01),
      median_profit_margin_no_govt_grants = scales::percent(median_profit_margin_no_govt_grants,
                                                            accuracy = 0.01),
      median_days_cash_on_hand = scales::number(median_days_cash_on_hand),
      median_months_cash_on_hand = scales::number(median_months_cash_on_hand),
      lessthan30days_cash_on_hand = scales::percent(lessthan30days_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01),
      btwn30and90days_cash_on_hand = scales::percent(btwn30and90days_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01),
      morethan90days_cash_on_hand = scales::percent(morethan90days_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01),
      lessthan1month_cash_on_hand = scales::percent(lessthan1month_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01),
      btwn1and3months_cash_on_hand = scales::percent(btwn1and3months_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01),
      morethan3months_cash_on_hand = scales::percent(morethan3months_cash_on_hand / number_reporting_govt_grants, accuracy = 0.01)
    ) |> 
    dplyr::select(
      num_nonprofits,
      number_reporting_govt_grants,
      total_govt_grants,
      median_profit_margin,
      median_profit_margin_no_govt_grants,
      median_days_cash_on_hand,
      lessthan30days_cash_on_hand,
      btwn30and90days_cash_on_hand,
      morethan90days_cash_on_hand,
      median_months_cash_on_hand,
      lessthan1month_cash_on_hand,
      btwn1and3months_cash_on_hand,
      morethan3months_cash_on_hand
    ) |>
    dplyr::rename(
      "Total Number of Nonprofits" = num_nonprofits,
      "Number of 990 Filers reporting Government Grants" = number_reporting_govt_grants,
      "Total Government Grants ($USD)" = total_govt_grants,
      "Median Profit Margin (%)" = median_profit_margin,
      "Median Profit Margin (Without Government Grants) (%)" = median_profit_margin_no_govt_grants,
      "Median Days Cash on Hand" = median_days_cash_on_hand,
      "% With Less than 30 Days Cash on Hand" = lessthan30days_cash_on_hand,
      "% With Between 30 and 90 Days Cash on Hand" = btwn30and90days_cash_on_hand,
      "% With More than 90 Days Cash on Hand" = morethan90days_cash_on_hand,
      "Median Months Cash on Hand" = median_months_cash_on_hand,
      "% With Less than 1 Month Cash on Hand" = lessthan1month_cash_on_hand,
      "% With Between 1 and 3 Months Cash on Hand" = btwn1and3months_cash_on_hand,
      "% With More than 3 Months Cash on Hand" = morethan3months_cash_on_hand
    )
  
  return(state_summary)
}


#' Summarize Nonprofit Financial Data
#'
#' @param data The input dataframe
#' @param group_var Optional grouping variable name as string (e.g., "CENSUS_STATE_NAME")
#' @param group_var_rename Optional new name for the grouping variable (e.g., "State")
#'
#' @return A dataframe with summarized nonprofit financial metrics
summarize_nonprofit_data <- function(data, group_var = NULL, group_var_rename = NULL, qc = FALSE) {
  
  # Start the pipeline
  summary <- data
  
  # Add grouping if specified
  if (qc){
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
  if (qc){
    summary <- summary |>
      dplyr::select(
        CENSUS_STATE_NAME,
        !!sym(group_var),
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
  if (qc == FALSE){
    summary <- summary |>
      dplyr::mutate(
        total_govt_grants = scales::dollar(total_govt_grants),
        median_profit_margin = scales::percent(median_profit_margin, accuracy = 0.01),
        median_profit_margin_no_govt_grants = scales::percent(median_profit_margin_no_govt_grants, accuracy = 0.01),
        proportion_at_risk = scales::percent(proportion_at_risk, accuracy = 0.01)
      )
  }
  
  # Rename columns
  rename_list <- list(
    "No. of 990 Filers w/ Gov Grants" = "num_990filers_govgrants",
    "Total Gov Grants ($)" = "total_govt_grants",
    "Operating Surplus (%)" = "median_profit_margin",
    "Operating Surplus w/o Gov Grants (%)" = "median_profit_margin_no_govt_grants",
    "Share of 990 Filers w/ Gov Grants at Risk" = "proportion_at_risk"
  )
  
  # Add group variable rename if provided
  if (!is.null(group_var) && !is.null(group_var_rename)) {
    rename_list[[group_var_rename]] <- group_var
  }
  
  summary <- summary |>
    dplyr::rename(!!!rename_list)
  
  return(summary)
}
