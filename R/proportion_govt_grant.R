proportion_govt_grant <- function(govt_grant, total_revenue) {
  
  if (is.na(total_revenue)){
    proportion <- NA
  } else if (is.na(govt_grant)){
    proportion <- 0
  } else if (total_revenue != 0 && govt_grant != 0) {
    proportion <- govt_grant / total_revenue
  } else {
    proportion <- 0
  }
  return(proportion)
}
