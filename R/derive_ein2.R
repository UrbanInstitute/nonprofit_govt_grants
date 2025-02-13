# Script containing function to derive EIN2

#' @title Format EIN to EIN2
#' @param ein 9 character original EIN
#' @returns character scalar EIN2. EIN-XX-XXXXXXX
derive_ein2 <- function(ein){
  ein2 <- format_ein(ein)
  ein2 <- paste0("EIN-", substr(ein2, 1, 2), "-", substr(ein2, 3, 9))
  return(ein2)
}

#' @title Function to format EIN to 9 characters long
#' @description Appends leading zeros until EIN has 9 characters
#' @param ein integer scalar. Original EIN
#' @returns character scalar. EIN with 9 characters.
format_ein <- function(ein) {
  if (is.na(ein)){
    ein <- "000000000"
    return(ein)
  } else {
    ein_len <- nchar(ein)
    if (ein_len == 9){
      return(ein)
    } else {
      diff = 9 - ein_len
      diff = rep("0", diff)
      diff = paste0(diff, collapse = "")
      ein <- paste0(diff, ein, collapse = "")
      return(ein)
    }
  }
}

format_ein <- function( x, to="id" ) {
  if( to == "id" )
  {   
    x <- stringr::str_pad( x, 9, side="left", pad="0" )
    sub1 <- substr( x, 1, 2 )
    sub2 <- substr( x, 3, 9 )
    ein  <- paste0( "EIN-", sub1, "-", sub2 ) 
    return(ein)
  }
  
  if( to == "n" )
  {  
    x <- gsub( "[^0-9]", "", x )
    return( x )
  }
  
}
