#' Function to import \code{`summaries`} table from a \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' 
#' @author Stuart K. Grange
#' 
#' @return Data frame. 
#' 
#' @examples 
#' \dontrun{
#' 
#' # Import summaries from a smonitor database
#' data_summaries <- import_summaries(con)
#' 
#' }
#' 
#' @export
import_summaries <- function(con) {
  
  # Get look-up table
  df <- databaser::db_get(
    con, 
    "SELECT summaries.*,
    sites.site_name
    FROM summaries
    LEFT JOIN sites
    ON summaries.site = sites.site
    ORDER BY summaries.process,
    summaries.summary"
  )
  
  return(df)
  
}
