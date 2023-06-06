#' Function to set invalid values to missing (\code{NA}) based on 
#' \strong{smonitor}'s convention for validity. 
#' 
#' Validity integers less-than and equal to zero are considered invalid and will
#' be set to missing and validity integers that are either missing or positive 
#' are considered valid. 
#' 
#' @author Stuart K. Grange
#' 
#' @param value A numeric vector representing values (usually 
#' measurements/observations).
#' 
#' @param validity An integer vector representing validity. 
#' 
#' @param include_zero Should validities of \code{0} be considered invalid and
#' the corresponding values be set to \code{NA}? \code{0} is used for calibration
#' activities and sometimes these observations are needed, but other invalidity
#' types are not.
#' 
#' @export
set_invalid_values <- function(value, validity, include_zero = TRUE) {
  
  if (include_zero) {
    value <- if_else(validity <= 0L & !is.na(validity), NA_real_, as.numeric(value))
  } else {
    value <- if_else(validity <= -1L & !is.na(validity), NA_real_, as.numeric(value))
  }
  
  return(value)
  
}
