operating_margin <- function(netoperatingincome, total_revenue, deprecn = 0){
  netoperatingincome <- ifelse(is.na(netoperatingincome), 0, netoperatingincome)
  total_revenue <- ifelse(is.na(total_revenue), 0, total_revenue)
  deprecn <- ifelse(is.na(deprecn), 0, deprecn)
  
  numerator <- netoperatingincome + deprecn
  denominator <- total_revenue
  
  if(denominator == 0){
    operating_margin <- NA
  } else if (numerator == 0) {
    operating_margin <- 0
  } else {
    operating_margin <- numerator / denominator
  }
  return(operating_margin)
}