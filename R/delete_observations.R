#' Function to delete observations from the \code{`observations`} table in a
#' \strong{smonitor} database. 
#' 
#' \code{delete_observations} is generally used immediately before a database 
#' insert so old observations are deleted before a new set of observations are 
#' inserted. This process avoids duplicate observations which are almost always
#' undesirable. 
#' 
#' \code{delete_observations} does allow the use of groups based on the input 
#' data frame.
#' 
#' @author Stuart K. Grange
#' 
#' @seealso \code{\link{db_execute}}, \code{\link{insert_observations}}
#' 
#' @param con Database connection.
#' 
#' @param df Data frame; usually an object which is about to be inserted into 
#' a database table. \code{delete_observations} uses this as a mapping table to
#' know what to send the database in the delete statement.
#' 
#' @param groups Groups in \code{df} to apply the deleting function to. Default 
#' is \code{c("process", "summary")} and should not need changing. 
#' 
#' @param match Type of match to use for dates. \code{"between"} and 
#' \code{"all"} are supported. Beware that \code{"all"} will remove all 
#' observations associated with a process. 
#' 
#' @param convert Should dates be coverted to integers for the unix time/date 
#' match for the database? Default is \code{TRUE} but is not necessary if dates 
#' in \code{df} are already in unix time format. 
#' 
#' @param progress Type of progress bar to display. Default is \code{"time"}. 
#' 
#' @export
delete_observations <- function(con, df, groups = c("process", "summary"), 
                                match = "between", convert = TRUE, 
                                progress = "time") {
  
  # Check data frame input
  if (!all(groups %in% names(df))) 
    stop("Data frame must contain 'group' variables.", call. = FALSE)
  
  # May need to use the argument
  if (any(is.na(df[, "process"])))
    stop("Data frame must not contain missing processes.", call. = FALSE)
  
  if (match == "between") {
    
    # Delete observations by groups
    plyr::d_ply(df, groups, function(x) 
      delete_observations_worker(con, x, match = match, convert = convert), 
      .progress = progress)
    
  }
  
  if (match == "all") {
    
    # Get processes
    keys <- unique(df$process)
   
    # Delete all observations associated with a process
    delete_process(con, keys)
    
  }
 
  # No return
  
}


# No export
delete_observations_worker <- function(con, df, match, convert) {
  
  # Drop tbl_df
  df <- threadr::base_df(df)
  
  if (match == "between") {
    
    # Get keys
    process <- df$process[1]
    summary <- df$summary[1]
    
    # Get dates, to-do: is avoiding nas ok? 
    date_min <- min(df$date, na.rm = TRUE)
    date_max <- max(df$date, na.rm = TRUE)
    
    # Convert if needed
    if (convert) {
      
      date_min <- as.integer(date_min)
      date_max <- as.integer(date_max)
      
    }
    
    # Build statement
    sql <- stringr::str_c(
      "DELETE FROM observations 
      WHERE process = ", process, 
      " AND summary = ", summary, 
      " AND date BETWEEN ", date_min, " AND ", date_max)
    
    # Clean
    sql <- threadr::str_trim_many_spaces(sql)
    
    # Use statement
    databaser::db_execute(con, sql)
    
  }
  
  # No return
  
}
