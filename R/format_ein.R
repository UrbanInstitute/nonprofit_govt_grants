#' @title Format EIN (Employer Identification Number)
#' @description Formats EIN either as a formatted string (XX-XXXXXXX) or extracts just the numeric values
#' @param x character or numeric scalar. Original EIN value
#' @param to character scalar. Output format: "id" for formatted EIN-XX-XXXXXXX, "n" for numeric only
#' @returns character scalar. Either formatted EIN string or numeric characters only
#' @examples
#' format_ein("123456789", to="id")  # Returns "EIN-12-3456789"
#' format_ein("EIN-12-3456789", to="n")  # Returns "123456789"
format_ein <- function(x, to="id") {
  if(to == "id") {   
    x <- stringr::str_pad(x, 9, side="left", pad="0")
    sub1 <- substr(x, 1, 2)
    sub2 <- substr(x, 3, 9)
    ein  <- paste0("EIN-", sub1, "-", sub2) 
    return(ein)
  }
  
  if(to == "n") {  
    x <- gsub("[^0-9]", "", x)
    return(x)
  }
}


