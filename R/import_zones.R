#' Function to import zones from an \strong{smonitor} database. 
#' 
#' @param con Database connection. 
#' @param zone A zone vector. If not used, all zones will be returned. 
#' 
#' @return SpatialPolygonsDataFrame with WGS84 projection. 
#' 
#' @seealso \code{\link{import_by_zone}}, \code{\link{import_zone_names}}
#' 
#' @author Stuart K. Grange
#' 
#' @export
import_zones <- function(con, zone = NA) {
  
  if (is.na(zone)[1]) {
    
    # Get all sites
    sql <- "SELECT * FROM zones ORDER BY zones"
    df <- databaser::db_get(con, sql)
    
  } else {
    
    # Format
    zone <- threadr::str_sql_quote(zone)
    
    sql <- stringr::str_c("SELECT * 
                           FROM zones 
                           WHERE zone IN (", zone, ")")
    
    sql <- threadr::str_trim_many_spaces(sql)
    
    # Query
    df <- databaser::db_get(con, sql)
    
  }
  
  # Promote
  sp <- gissr::sp_from_wkt(df, projection = gissr::projection_wgs84())
  
  # Return
  sp
  
}
