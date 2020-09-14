#' Function to import trend summaries from an \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' 
#' @param site Sites to import. 
#' 
#' @param variable Variables to import. 
#' 
#' @param aggregation Aggregation to import. 
#' 
#' @param date_insert Should the \code{date_insert} variable be returned? 
#' 
#' @param spread Should the return be reshaped to have variables as column 
#' names? The \code{date_insert} and \code{count} variables are dropped so this
#' can occur. 
#' 
#' @param tz What time-zone should the dates be returned as? Default is 
#' \code{"UTC"}. 
#' 
#' @param print_query Should the SQL query statement be printed? 
#' 
#' @author Stuart K. Grange
#' 
#' @return Data frame.
#' 
#' @export
import_trend_summaries <- function(con, site = NA, variable = NA, 
                                   aggregation = "monthly_mean",
                                   date_insert = FALSE, spread = FALSE,
                                   tz = "UTC", print_query = FALSE) {
  .Defunct(
    msg = "`import_trend_summaries` is no longer available, please use `import_simmple_summaries`."
    )
  
}
