#' Function to create entries for the \code{`summary`} table in a 
#' \strong{smonitor} database. 
#' 
#' @author Stuart K. Grange
#' 
#' @param con Database connection. 
#' 
#' @param process A process integer. 
#' 
#' @param type Type of entries to create. Can be either \code{"standard"} or
#' \code{"extended"}.
#' 
#' @param validity_threshold Threshold for valid average. 
#' 
#' @export
create_summary_entries <- function(con, process, type = "standard", 
                                   validity_threshold = 0L) {
  
  # Query and select
  df_processes <- import_processes(con) %>% 
    dplyr::select(process, 
                  site, 
                  variable)
  
  # Filter
  df_processes <- df_processes[df_processes$process %in% process, ]
  
  if (type == "standard") {
    
    df_agg <- import_aggregations(con) %>% 
      dplyr::filter(summary %in% c(20, 21, 22, 23, 40, 41, 42, 43)) %>% 
      threadr::replicate_rows(length(process))
    
  }
  
  if (type == "extended") {
    
    df_agg <- import_aggregations(con) %>% 
      dplyr::filter(summary %in% c(1, 4, 20, 21, 22, 23, 40, 41, 42, 43)) %>% 
      threadr::replicate_rows(length(process))
    
  }
  
  # Will be recycled
  df_agg$process <- process
  
  df_agg <- df_agg %>% 
    dplyr::left_join(df_processes, "process") %>% 
    dplyr::select(process, 
                  site,
                  variable,
                  summary_name) %>% 
    dplyr::mutate(validity_threshold = validity_threshold) %>% 
    dplyr::arrange(process)
  
  # Drop nonsense wind direction summaries
  df_agg <- df_agg %>% 
    dplyr::filter(!(variable == "wd" & grepl("_max|_min", summary_name)))
  
  # Return
  df_agg
  
}
