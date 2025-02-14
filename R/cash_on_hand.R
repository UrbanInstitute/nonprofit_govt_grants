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


#' Calculate Months Cash on Hand
#'
#' This function calculates the months cash on hand metric for a nonprofit organization
#' based on their liquid assets and expenses. NA values in intermediate calculations
#' are treated as 0.
#'
#' @param unrestrict_eoy Unrestricted net assets, end of year
#' @param land_bldg_net_eoy Net value of land and buildings, end of year
#' @param tax_exempt_bond_eoy Tax-exempt bond liabilities, end of year
#' @param mtg_note_eoy Mortgage and notes payable, end of year
#' @param note_unsec_eoy Unsecured notes payable, end of year
#' @param exp_tot_cy Total expenses, current year
#' @param exp_deprec_tot Total depreciation expenses
#'
#' @return Numeric value representing months cash on hand. Returns 0 if liquid assets are 0,
#'         NA if expenses are 0.
#'
#' @examples
#' months_cash_on_hand(
#'   unrestrict_eoy = 1000000,
#'   land_bldg_net_eoy = 500000,
#'   tax_exempt_bond_eoy = 100000,
#'   mtg_note_eoy = 50000,
#'   note_unsec_eoy = 25000,
#'   exp_tot_cy = 200000,
#'   exp_deprec_tot = 10000
#' )
months_cash_on_hand <- function(unrestrict_eoy,
                                land_bldg_net_eoy,
                                tax_exempt_bond_eoy,
                                mtg_note_eoy,
                                note_unsec_eoy,
                                exp_tot_cy,
                                exp_deprec_tot) {
  
  
  # Replace NA with 0 for all input values
  unrestrict_eoy <- ifelse(is.na(unrestrict_eoy), 0, unrestrict_eoy)
  land_bldg_net_eoy <- ifelse(is.na(land_bldg_net_eoy), 0, land_bldg_net_eoy)
  tax_exempt_bond_eoy <- ifelse(is.na(tax_exempt_bond_eoy), 0, tax_exempt_bond_eoy)
  mtg_note_eoy <- ifelse(is.na(mtg_note_eoy), 0, mtg_note_eoy)
  note_unsec_eoy <- ifelse(is.na(note_unsec_eoy), 0, note_unsec_eoy)
  
  # Calculate liquid assets
  liquid_assets <- unrestrict_eoy - (
    land_bldg_net_eoy - 
      tax_exempt_bond_eoy - 
      mtg_note_eoy - 
      note_unsec_eoy
  )
  
  # Calculate expenses (treating NA as 0)
  exp_deprec_tot <- ifelse(is.na(exp_deprec_tot), 0, exp_deprec_tot)
  exp_tot_cy <- ifelse(is.na(exp_tot_cy), 0, exp_tot_cy)
  expenses <- exp_tot_cy - exp_deprec_tot
  
  # Check for zero expenses first
  if (is.na(expenses) || expenses == 0) {
    return(NA)
  }
  
  # Check for zero or NA liquid assets
  if (is.na(liquid_assets)) {
    return(NA)
  } else if (liquid_assets == 0) {
    return(0)
  }
  
  # Calculate final result
  cash_on_hand <- 12 * liquid_assets / expenses
  
  return(cash_on_hand)
}
