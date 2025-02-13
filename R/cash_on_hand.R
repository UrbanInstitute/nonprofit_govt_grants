#' Calculate Days or Months of Cash
#' 
#' @description
#' Calculates the number of days or months of cash on hand for a nonprofit organization
#' using Form 990 data. Cash includes cash, savings, pledges receivable, and accounts
#' receivable. Expenses are calculated as total expenses minus depreciation.
#' 
#' @details
#' Form 990 fields used:
#' - Cash: Part X, line 1B
#' - Savings: Part X, line 2B
#' - Pledges: Part X, line 3B
#' - Accounts Receivable: Part X, line 4B
#' - Total Expenses: Part IX, line 25A
#' - Depreciation: Part IX, line 22A
#'
#' @param cash_eoy End of year cash (Part X, line 1B)
#' @param savings_eoy End of year savings (Part X, line 2B)
#' @param pledges_eoy End of year pledges receivable (Part X, line 3B)
#' @param receivables_eoy End of year accounts receivable (Part X, line 4B)
#' @param total_expenses Total expenses (Part IX, line 25A)
#' @param depreciation Depreciation expense (Part IX, line 22A)
#' @param unit Character. Either "days" or "months" (default = "months")
#'
#' @return Numeric value representing either days or months of cash on hand
#'
#' @examples
#' # Calculate months of cash
#' calculate_cash_duration(
#'   cash_eoy = 100000,
#'   savings_eoy = 50000,
#'   pledges_eoy = 25000,
#'   receivables_eoy = 25000,
#'   total_expenses = 400000,
#'   depreciation = 20000,
#'   unit = "months"
#' )
#' 
#' # Calculate days of cash
#' calculate_cash_duration(
#'   cash_eoy = 100000,
#'   savings_eoy = 50000,
#'   pledges_eoy = 25000,
#'   receivables_eoy = 25000,
#'   total_expenses = 400000,
#'   depreciation = 20000,
#'   unit = "days"
#' )
calculate_cash_duration <- function(
    cash_eoy,
    savings_eoy,
    pledges_eoy,
    receivables_eoy,
    total_expenses,
    depreciation,
    unit = "months"
) {
  # Input validation
  if (!unit %in% c("days", "months")) {
    stop("unit must be either 'days' or 'months'")
  }
  
  # Calculate total cash and cash-like assets
  cash_ish <- sum(cash_eoy, savings_eoy, pledges_eoy, receivables_eoy, na.rm = TRUE)
  
  # Handle NA values in expense calculation
  if (is.na(total_expenses)) {
    return(NA_real_)
  }
  
  # If depreciation is NA, treat it as 0
  depreciation <- if(is.na(depreciation)) 0 else depreciation
  
  # Calculate expenses per period (excluding depreciation)
  net_expenses <- total_expenses - depreciation
  
  # Avoid division by zero
  if (net_expenses == 0) {
    return(NA_real_)
  }
  
  # Calculate based on specified unit
  divisor <- if(unit == "months") 12 else 365
  
  return(cash_ish / (net_expenses / divisor))
}
