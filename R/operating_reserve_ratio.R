#' Calculate Operating Reserve Ratio
#'
#' This function calculates the operating reserve ratio for a nonprofit organization
#' based on their total net assets and total expenses.
#'
#' @param net_assets_eoy Total net assets, end of year
#' @param total_expenses Total expenses, current year
#'
#' @return Numeric value representing operating reserve ratio. 
#'         Returns NA if expenses are 0 or NA.
#'         Returns 0 if net assets are 0.
#'
#' @examples
#' calc_operating_reserve_ratio(
#'   net_assets_eoy = 1000000,
#'   total_expenses = 200000
#' )
operating_reserve_ratio <- function(net_assets_eoy, total_expenses) {
  # Check for zero expenses first
  if (is.na(total_expenses) || total_expenses == 0) {
    return(NA)
  }
  
  # Check for zero or NA net assets
  if (is.na(net_assets_eoy)) {
    return(NA)
  } else if (net_assets_eoy == 0) {
    return(0)
  }
  
  # Calculate ratio
  ratio <- net_assets_eoy / total_expenses
  
  return(ratio)
}