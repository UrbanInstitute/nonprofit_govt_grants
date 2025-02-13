#' Calculate Profit Margin
#' 
#' @param revenue Numeric value of total revenue - F9_08_REV_TOT_TOT
#' @param expenses Numeric value of total expenses - F9_09_EXP_TOT_TOT
#' @param govtgrants Numeric value of government grants (optional) - F9_08_REV_CONTR_GOVT_GRANT
#' @return Numeric value representing profit margin (net_income/revenue)
#' @examples
#' profit_margin(100, 80) # Returns 0.2
#' profit_margin(100, 80, 10) # Returns 0.1
#' profit_margin(0, 100) # Returns 0
#' profit_margin(NA, 100) # Returns NA
profit_margin <- function(revenue, expenses, govtgrants = NA) {
  # Early return for NA revenue
  if (is.na(revenue)) return(NA)
  
  # Early return for zero revenue
  if (revenue == 0) return(0)
  
  # Calculate net income
  net_income <- dplyr::case_when(
    all(is.na(c(revenue, expenses))) ~ NA_real_, # NA if all are NA
    is.na(revenue) ~ -expenses, # if expenses provided but no revenue, assume 0 revenue
    is.na(expenses) ~ revenue, #if revenue provided but no expenses, assume 0 expenses
    TRUE ~ revenue - expenses
  )
  
  # Subtract government grants if specified in function
  if (!is.na(net_income) && !is.na(govtgrants)) {
    net_income <- net_income - govtgrants
  }
  
  # Calculate profit margin
  if (is.na(net_income) || net_income == 0) {
    return(0)
  }
  
  return(net_income / revenue)
}

# Add test cases
test_cases <- function() {
  stopifnot(
    profit_margin(100, 80) == 0.2,
    profit_margin(100, 80, 10) == 0.1,
    profit_margin(0, 100) == 0,
    is.na(profit_margin(NA, 100)),
    profit_margin(100, NA) == 1,
    profit_margin(100, 100) == 0
  )
  cat("All test cases passed!\n")
}
