days_cash_on_hand <- function(ASSET_CASH_EOY,
                              ASSET_SAVING_EOY,
                              ASSET_PLEDGE_NET_EOY,
                              ASSET_ACC_NET_EOY,
                              EXP_TOT_CY,
                              EXP_DEPREC_TOT) {
  # Replace NA values with 0
  ASSET_CASH_EOY <- tidyr::replace_na(ASSET_CASH_EOY, 0)
  ASSET_SAVING_EOY <- tidyr::replace_na(ASSET_SAVING_EOY, 0)
  ASSET_PLEDGE_NET_EOY <- tidyr::replace_na(ASSET_PLEDGE_NET_EOY, 0)
  ASSET_ACC_NET_EOY <- tidyr::replace_na(ASSET_ACC_NET_EOY, 0)
  EXP_TOT_CY <- tidyr::replace_na(EXP_TOT_CY, 0)
  EXP_DEPREC_TOT <- tidyr::replace_na(EXP_DEPREC_TOT, 0)
  
  # Calculate days cash on hand
  numerator <- ASSET_CASH_EOY + ASSET_SAVING_EOY + ASSET_PLEDGE_NET_EOY + ASSET_ACC_NET_EOY
  denominator <- (EXP_TOT_CY - EXP_DEPREC_TOT) / 365
  
  if (denominator == 0) {
    CASH_ON_HAND_DAYS <- NA
  } else if (numerator == 0) {
    CASH_ON_HAND_DAYS <- 0
  } else {
    CASH_ON_HAND_DAYS <- numerator / denominator
  }
  
  return(CASH_ON_HAND_DAYS)
}
