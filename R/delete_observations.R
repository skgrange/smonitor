#' Function to delete observations from the \code{`observations`} table in a
#' \strong{smonitor} database. 
#' 
#' \code{delete_observations} is generally used immediately before a database 
#' insert so old observations are deleted before a new set of observations are 
#' inserted. This process avoids duplicate observations which are almost always
#' undesirable. 
#' 
#' \code{delete_observations} allows the use of groups based on the input data
#' frame.
#' 
#' @author Stuart K. Grange
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
#' @param verbose Should the function give messages? 
#' 
#' @param progress Type of progress bar to display. 
#' 
#' @return Invisible. A database delete. 
#' 
#' @seealso \code{\link{db_execute}}, \code{\link{insert_observations}}
#' 
#' @export
delete_observations <- function(con, df, groups = c("process", "summary"), 
                                match = "between", verbose = FALSE, 
                                progress = "none") {
  
  # Check data frame input
  if (!all(groups %in% names(df))) 
    stop("Input data frame must contain all `group` variables...", call. = FALSE)
  
  # May need to use the argument
  if (any(is.na(df[, "process"])))
    stop("Input data frame must not contain missing processes...", call. = FALSE)
  
  if (any(is.na(df[, "date"])))
    stop("Input data frame must not contain missing dates...", call. = FALSE)
  
  if (match == "between") {
    
    # Delete observations by groups
    plyr::d_ply(
      df, 
      groups, 
      function(x) delete_observations_worker(
        con, 
        x, 
        match = match,
        verbose = verbose
      ),
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
delete_observations_worker <- function(con, df, match, verbose) {
  
  # Drop tbl_df
  df <- threadr::base_df(df)
  
  if (match == "between") {
    
    # Get keys
    process <- df$process[1]
    summary <- df$summary[1]
    
    # Get dates
    date_min <- min(df$date, na.rm = TRUE)
    date_max <- max(df$date, na.rm = TRUE)
    
    # Convert if needed
    if (lubridate::is.POSIXt(date_min)) date_min <- as.numeric(date_min)
    if (lubridate::is.POSIXt(date_max)) date_max <- as.numeric(date_max)
    
    # Build statement
    sql <- stringr::str_c(
      "DELETE FROM observations 
      WHERE process = ", process, 
      " AND summary = ", summary, 
      " AND date BETWEEN ", date_min, " AND ", date_max
    )
    
    # Clean
    sql <- threadr::str_trim_many_spaces(sql)
    
    # Message to user
    if (verbose) {
      
      date_min_format <- threadr::parse_unix_time(date_min)
      date_max_format <- threadr::parse_unix_time(date_min)
      date_min_format <- format(date_min_format, usetz = TRUE)
      date_max_format <- format(date_max_format, usetz = TRUE)
      
      list_message <- list(
        message = "Deleting observations...",
        date_system = format(lubridate::now(), usetz = TRUE),
        process = process,
        summary = summary,
        date_span = c(date_min_format, date_max_format)
      )
      
      message(threadr::to_json(list_message))
      
    }
    
    
    # Use statement
    databaser::db_execute(con, sql)
    
  }
  
  # No return
  
}
