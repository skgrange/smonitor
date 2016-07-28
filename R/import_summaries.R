#' Function to import \code{`summaries`} table from a \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' @param extra Return extra data? Default is \code{TRUE}.
#' 
#' @export
import_summaries <- function(con, extra = TRUE) {
  
  # Get look-up table
  df <- databaser::db_get(con, "SELECT summaries.*,
                          sites.site_name
                          FROM summaries
                          LEFT JOIN sites
                          ON summaries.site = sites.site
                          ORDER BY summaries.process, 
                          summaries.summary")
  
  # Only a few variables
  if (!extra)
    df <- df[, c("process", "summary", "summary_name", "site", "site_name", 
                 "variable", "source", "validity_threshold", "period")]
  
  # Return
  df
  
}
