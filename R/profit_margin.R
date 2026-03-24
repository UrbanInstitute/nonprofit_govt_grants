#' Calculate Profit Margin (scalar)
#'
#' @param revenue Numeric value of total revenue
#' @param expenses Numeric value of total expenses
#' @param govtgrants Numeric value of government grants (optional)
#' @return Numeric value representing profit margin (net_income/revenue)
#' @examples
#' profit_margin(100, 80) # Returns 0.2
#' profit_margin(100, 80, 10) # Returns 0.1
#' profit_margin(0, 100) # Returns 0
#' profit_margin(NA, 100) # Returns NA
profit_margin <- function(revenue, expenses, govtgrants = NA) {
  if (is.na(revenue)) return(NA)
  if (revenue == 0) return(0)

  net_income <- dplyr::case_when(
    all(is.na(c(revenue, expenses))) ~ NA_real_,
    is.na(revenue) ~ -expenses,
    is.na(expenses) ~ revenue,
    TRUE ~ revenue - expenses
  )

  if (!is.na(net_income) && !is.na(govtgrants)) {
    net_income <- net_income - govtgrants
  }

  if (is.na(net_income) || net_income == 0) return(0)

  return(net_income / revenue)
}

#' Calculate Profit Margin (vectorized)
#'
#' Vectorized companion to `profit_margin()`. Operates on entire columns at
#' once, eliminating the need for `rowwise()` or `pmap()`.
#'
#' @param revenue Numeric vector of total revenue
#' @param expenses Numeric vector of total expenses
#' @param govtgrants Numeric vector of government grants (default NA)
#' @return Numeric vector of profit margins
profit_margin_vec <- function(revenue, expenses, govtgrants = NA) {
  net_income <- dplyr::case_when(
    is.na(revenue) & is.na(expenses) ~ NA_real_,
    is.na(revenue)  ~ -expenses,
    is.na(expenses) ~ revenue,
    TRUE            ~ revenue - expenses
  )

  # Subtract government grants where both net_income and govtgrants are non-NA
  has_grants <- !is.na(net_income) & !is.na(govtgrants)
  net_income <- ifelse(has_grants, net_income - govtgrants, net_income)

  result <- dplyr::case_when(
    is.na(revenue)                   ~ NA_real_,
    revenue == 0                     ~ 0,
    is.na(net_income) | net_income == 0 ~ 0,
    TRUE                             ~ net_income / revenue
  )

  result
}
