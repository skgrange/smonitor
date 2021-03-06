#' Function to import data from an \strong{smonitor} database based on zones/
#' spatial polygons. 
#' 
#' \code{import_by_zone} needs a \code{`zones`} table and has been developed 
#' specifically for \strong{smonitor} Europe and it therefore not generic.
#' 
#' @param con Database connection. 
#' 
#' @param zone A vector of zones to use as the boundaries for site filtering. 
#' Use \code{\link{import_zone_names}} to get the vectors needed.
#' 
#' @param variable An optional variable vector. If not used, all variables will
#' be selected and returned. 
#' 
#' @param sp_other An optional spatial polygon which exists outside the database
#' tables to be used as a boundary rather than using \code{zone}. 
#' 
#' @param start Start date to import. 
#' 
#' @param end End date to import. 
#' 
#' @param period Averaging period. Default is \code{"hour"}. \code{period} can
#' also take the value \code{"any"} which will return all periods, but 
#' \code{pad} argument will be ignored.
#' 
#' @param pad Should the time-series be padded to ensure all dates in the 
#' observation period are present? Default is \code{TRUE}. 
#' 
#' @param spread Should the data frame take the wider format resulting from
#' spreading the data? Default is \code{FALSE}. 
#' 
#' @param tz Time-zone for the dates to be parsed into. Default is \code{"UTC"}.
#' 
#' @param valid_only Should only valid data be returned? Default is \code{TRUE}. 
#' 
#' @param site_name Should \code{site_name} be returned? 
#' 
#' @author Stuart K. Grange
#' 
#' @return Tibble. 
#' 
#' @seealso \code{\link{import_zones}}, \code{\link{import_zone_names}}
#' 
#' @export
import_by_zone <- function(con, zone, variable = NA, sp_other = NA, start = 1969, 
                           end = NA, period = "hour", pad = TRUE, spread = TRUE, 
                           tz = "UTC", valid_only = TRUE, site_name = FALSE) {
  
  # Get sites
  suppressWarnings(
    sp_sites <- import_sites(con) %>% 
      gissr::sp_from_wkt(projection = gissr::projection_wgs84())
  )
  
  # Use zone vector
  if (suppressWarnings(is.na(sp_other))) {
    
    # Build sql
    zone <- threadr::str_sql_quote(zone)
    
    sql <- stringr::str_c(
      "SELECT * 
      FROM zones 
      WHERE zone IN (", zone, ")"
    )
    
    sql <- stringr::str_squish(sql)
    
    # Get zones
    sp_zones <- databaser::db_get(con, sql) %>% 
      gissr::sp_from_wkt(projection = gissr::projection_wgs84())
    
    # Filter sites
    sp_sites <- sp_sites[sp_zones, ]
    
  } else {
    
    # Check
    if (!grepl("polygon", gissr::sp_class(sp_other), ignore.case = TRUE)) {
      stop("'sp_other' needs to be a polygon.", call. = FALSE) 
    }
    
    if (!gissr::sp_projection(sp_sites) == gissr::sp_projection(sp_other)) {
      stop("Projection systems are not identical.", call. = FALSE) 
    }
    
    # Filter with extra polygon
    sp_sites <- sp_sites[sp_other, ]
    
  }
  
  # Get vector
  sites <- sort(unique(sp_sites@data$site))
  
  # Import observations
  df <- import_by_site(
    con, 
    site = sites, 
    variable = variable, 
    start = start,
    end = end, 
    period = period, 
    pad = pad, 
    spread = spread, 
    tz = tz, 
    valid_only = valid_only, 
    site_name = site_name
  )
  
  return(df)
  
}
