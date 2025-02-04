spending_on_hand <- function(netassets,
                             landbldgeqpmt,
                             taxexemptbnd,
                             mtgnotespayable,
                             totexpn,
                             deprcn) {
  
  numerator <- netassets - (landbldgeqpmt - taxexemptbnd - mtgnotespayable)
  denominator <- totexpn - deprcn
  
  if (denominator == 0) {
    months_spend_on_hand <- NA
  } else if (numerator == 0) {
    months_spend_on_hand <- 0
  } else {
    months_spend_on_hand <- 12 * (numerator / denominator)
  }
  return(months_spend_on_hand)
}
