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
#' @export
set_invalid_values <- function(value, validity) {
  if_else(validity <= 0L & !is.na(validity), NA_real_, as.numeric(value))
}
