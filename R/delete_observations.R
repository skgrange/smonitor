#' Function to delete observations from the \code{`observations`} table in a
#' \strong{smonitor} database. 
#' 
#' \code{delete_observations} is generally used immediately before a database 
#' insert so old observations are deleted before a new set of observations are 
#' inserted. This process avoids duplicate observations which are almost always
#' undesirable. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection.
#' 
#' @param df Input tibble of observations. Usually an object which is about to 
#' be inserted into the \code{`observations`} database table. 
#' \code{delete_observations} uses this as a mapping table to know what to send
#' the database in the delete statement.
#' 
#' @param match Type of match to use for dates. \code{"between"} and 
#' \code{"all"} are supported. Beware that \code{"all"} will remove all 
#' observations associated with a process. 
#' 
#' @param verbose Should the function give messages? 
#' 
#' @param progress Should a progress bar be displayed? 
#' 
#' @return Invisible \code{con}.
#' 
#' @seealso \code{\link[databaser]{db_execute}}, 
#' \code{\link{insert_observations}}
#' 
#' @export
delete_observations <- function(con, df, match = "between", verbose = FALSE,
                                progress = FALSE) {
  
  # Return immediately when input contains no observations
  if (nrow(df) == 0) {
    if (verbose) {
      cli::cli_alert_info(
        "{threadr::cli_date()} Input data has no observations, the database has not been touched..."
      )
    }
    return(invisible(con))
  }
  
  if (any(is.na(df$process))) {
    cli::cli_abort("Input data must not contain missing processes.")
  }
  
  if (any(is.na(df$date))) {
    cli::cli_abort("Input data must not contain missing dates.")
  }
  
  if (match == "between") {
    # Delete observations by groups
    df %>% 
      dplyr::group_split(process,
                         summary,
                         .keep = TRUE) %>% 
      purrr::walk(
        ~delete_observations_worker(
          con, 
          df = .x,
          verbose = verbose
        ),
        .progress = progress
      )
  } else if (match == "all") {
    # Delete all observations associated with a process
    delete_process(con, unique(df$process))
  }
  
  return(invisible(con))
  
}


delete_observations_worker <- function(con, df, verbose) {
  
  # Get keys
  process <- df$process[1]
  summary <- df$summary[1]
  
  # Get dates
  date_min <- min(df$date)
  date_max <- max(df$date)
  
  # Convert if needed
  if (lubridate::is.POSIXt(date_min)) date_min <- as.numeric(date_min)
  if (lubridate::is.POSIXt(date_max)) date_max <- as.numeric(date_max)
  
  # Build statement
  sql_delete <- stringr::str_c(
    "DELETE FROM observations 
      WHERE process = ", process, 
    " AND summary = ", summary, 
    " AND date BETWEEN ", date_min, " AND ", date_max
  ) %>% 
    stringr::str_squish()
  
  # Message sql to user
  if (verbose) {
    cli::cli_alert_info(
      "{threadr::cli_date()} {sql_delete}"
    )
  }
  
  # Use statement
  databaser::db_execute(con, sql_delete)
  
  return(invisible(con))
  
}
