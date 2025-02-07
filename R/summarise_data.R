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
        scales::number,
        2
      )
    ) |> dplyr::select(
      !!sym(group_var),
      mean_months_cash_on_hand_tangibleassets,
      median_months_cash_on_hand_tangibleassets,
      mean_months_cash_on_hand_notangibleassets,
      median_months_cash_on_hand_notangibleassets,
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
