#' Function to test a time series with the Theil-Sen estimator. 
#' 
#' @param df Input data frame, containing time series observations. 
#' 
#' @param variable Variable name to test. 
#' 
#' @param deseason Should the time series be deseaonsalised before the trend 
#' test is conducted?
#' 
#' @param auto_correlation Should auto correlation be considered in the 
#' estimates?
#' 
#' @seealso \code{\link{TheilSen}}
#' 
#' @return Tibble with one observation/row. 
#' 
#' @author Stuart K. Grange
#' 
#' @export
theil_sen_trend_test <- function(df, variable = "value", deseason = FALSE, 
                                 auto_correlation = FALSE) {
  
  # Send plot to dev/null
  pdf(tempfile())
  
  # Do the test without any messages, quiet is for dplyr's progress bar
  quiet(
    df_test <- openair::TheilSen(
      df, 
      pollutant = variable,
      deseason = deseason,
      autocor = auto_correlation,
      plot = FALSE,
      silent = TRUE
    )$data$res2
  )
  
  dev.off()
  
  # Clean names of returned data frame, remove duplicates and add date variables
  df_test <- df_test %>% 
    setNames(stringr::str_replace_all(names(.), "\\.", "_")) %>% 
    filter(is.finite(conc)) %>% 
    mutate(date_start = min(df$date), 
           date_end = max(df$date))
  
  # Get n too
  n <- df %>% 
    filter(!is.na(!!variable)) %>% 
    pull(!!variable) %>% 
    length()
  
  # Select variables
  df_test <- df_test %>% 
    mutate(n = n) %>% 
    select(date_start,
           date_end,
           n,
           p_value = p,
           intercept,
           slope, 
           slope_lower = lower,
           slope_upper = upper)
  
  return(df_test)
  
}


quiet <- function(x) {
  sink(tempfile())
  on.exit(sink())
  invisible(force(x))
}
