#' Function to import \code{`summaries`} table from a \strong{smonitor} database. 
#' 
#' @param con Database connection to an \strong{smonitor} database.
#' 
#' @author Stuart K. Grange
#' 
#' @return Tibble. 
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
  
  databaser::db_get(
    con, 
    "SELECT summaries.*,
    sites.site_name
    FROM summaries
    LEFT JOIN sites
    ON summaries.site = sites.site
    ORDER BY summaries.process,
    summaries.summary"
  )

}
