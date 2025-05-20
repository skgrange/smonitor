#' Function to update validity variable in a \code{`observations`} table in a 
#' \strong{smonitor} database. 
#' 
#' \code{update_validity} does not update the validity variable with SQL, it 
#' queries the database, re-tests, deletes the old observations and inserts the
#' re-tested table. 
#' 
#' @param con Database connection. 
#' 
#' @param process A vector of processes. 
#' 
#' @param summary Summary to invalidate. This will usually be \code{0} for source
#' data and can only take one value. 
#' 
#' @param tz Time zone to conduct invalidations in. 
#' 
#' @param validity_action What action to be taken for the \code{validity} 
#' variable. \code{validity_action} can take the form of \code{"replace"} or 
#' \code{"update"}. See \code{\link{smonitor_observation_validity_test}} for
#' details.
#' 
#' @param verbose Should the function give messages? 
#' 
#' @author Stuart K. Grange
#' 
#' @return Invisible \code{con}.
#' 
#' @export
update_validity <- function(con, process, summary = 0, tz = "UTC", 
                            validity_action = "replace", verbose = FALSE) {
  
  # Check input
  if (length(summary) != 1) {
    stop("Only one summary can be used.", call. = FALSE)
  }
  
  # Get look-up table
  df <- import_invalidations(con, tz = tz) %>% 
    mutate(date_start = as.numeric(date_start),
           date_end = as.numeric(date_end),
           summary = summary)
  
  # Filter invalidations table
  df <- filter(df, process %in% !!process)
  
  # Check again
  if (nrow(df) != 0) {
    
    # Do
    df %>% 
      split(list(.$process, .$summary)) %>% 
      purrr::walk(
        ~update_validity_worker(
          con,
          df = .x,
          validity_action = validity_action,
          verbose = verbose
        )
      )
    
  } else {
    
    if (verbose) {
      message(
        threadr::date_message(), 
        "`invalidations` table contains no entires for given processes..."
      )
    }
    
  }
  
  return(invisible(con))
  
}


update_validity_worker <- function(con, df, validity_action, verbose) {
  
  # Get keys, will be single values from split input
  process <- df$process[1]
  summary <- df$summary[1]
  
  # Get observations
  if (verbose) message(threadr::date_message(), "Importing observations...")
  
  df_observations <- import_by_process(
    con, 
    process, 
    summary = summary, 
    start = 1900, 
    end = format(Sys.Date() + lubridate::years(5)), 
    valid_only = FALSE,
    warn = FALSE
  )
  
  if (nrow(df_observations) != 0) {
    
    # To unix dates
    df_observations <- df_observations %>% 
      mutate(date = as.numeric(date),
             date_end = as.numeric(date_end))
    
    # Update validity, look up table is filtered in function
    if (verbose) message(threadr::date_message(), "Applying invalidation...")
    df_observations <- smonitor_observation_validity_test(
      df_observations, df, validity_action = validity_action
    )
    
    # Insert new observations
    delete_observations(con, df_observations, match = "between", verbose = verbose)
    insert_observations(con, df_observations, verbose = verbose)
    
  } else {
    if (verbose) message(threadr::date_message(), "No observations to invalidate...")
  }
  
  return(invisible(con))
  
}


#' Function to add a validity variable to a data frame based on date ranges. 
#' 
#' @param df_observations Data frame to test and add or overwrite a 
#' \code{validity} variable. 
#' 
#' @param df A look-up data frame containing \code{process}, \code{date_start}, 
#' and \code{date_end} variables.
#' 
#' @param valid_value For values which are not invalid, \emph{i.e.} they fail 
#' the date range test, what value should they take? Default is \code{NA}, but
#' \code{1} could be appropriate too.  
#' 
#' @param validity_action What action to be taken for the \code{validity} 
#' variable. \code{validity_action} can take the form of \code{"replace"} or 
#' \code{"update"}. \code{"replace"} will replace the \code{validity} variable 
#' while \code{"update"} will preserve the current values of the \code{validity}
#' variable.
#' 
#' @seealso \code{\link[threadr]{within_range}}
#' 
#' @author Stuart K. Grange. 
#' 
#' @return Tibble. 
#' 
#' @export
smonitor_observation_validity_test <- function(df_observations, df, 
                                               valid_value = NA_integer_,
                                               validity_action = "replace") {
  
  # Check validity_action argument
  if (length(validity_action) == 0L || !validity_action %in% c("replace", "update")) {
    stop("`validity_action` must be `replace` or `update`.", call. = FALSE)
  }
  
  # Get keys from table
  # Get identifier
  process <- df_observations$process[1]
  
  # Filter look up table
  df <- filter(df, process == !!process)
  
  if (nrow(df) != 0) {
    
    # Do the range tests
    df_observations <- df_observations %>% 
      mutate(test = threadr::within_range(date, df$date_start, df$date_end))
    
    if (validity_action == "replace") {
      
      # Replace the entire vector
      df_observations <- df_observations %>% 
        mutate(validity = if_else(test, 0L, valid_value)) %>% 
        select(-test)
      
    } else if (validity_action == "update") {
      
      # Only update the vector, do not overwrite the current validity variable
      # TODO: might need to preserve any value that validity could have? 
      df_observations <- df_observations %>% 
        mutate(test_current = !is.na(validity) | validity == 1L,
               test_current = if_else(is.na(test_current), FALSE, test_current),
               validity = if_else(test | test_current, 0L, valid_value)) %>% 
        select(-test,
               -test_current)
      
    }
    
  } else {
    df_observations$validity <- valid_value
  }
  
  return(df_observations)
  
}
