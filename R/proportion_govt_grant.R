#' @title This function computes the proportion of revenue derived from government grants
#' @description Government grants from Part 08 Line 1e of the 2024 Form 990.
#' Total revenue from Part 08 Line 12 of the 2024 Form 990. With additional
#' handling of edge cases.
#' @param govt_grant numeric scalar of government grants
#' @param total_revenue numeric scalar of total revenue
#' @return numeric scalar of the proportion of revenue derived from government grants
proportion_govt_grant <- function(govt_grant, total_revenue) {
  if (all(is.na(c(govt_grant, total_revenue)))) return(NA_real_)
  else if (is.na(govt_grant) && !is.na(total_revenue)) return(0)
  else if (is.na(total_revenue) && !is.na(govt_grant)) return(NA_real_)
  
  
  proportion <- if (total_revenue != 0 && govt_grant != 0) {
    govt_grant / total_revenue
  } else {
    0
  }
  
  if (proportion < 0 || proportion > 1) return(NA_real_)
  
  proportion
}
