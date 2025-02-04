proportion_govt_grant <- function(govt_grant, total_revenue) {
  proportion <- 0
  if (total_revenue != 0 && govt_grant != 0) {
    proportion <- govt_grant / total_revenue
  }
  return(proportion)
}