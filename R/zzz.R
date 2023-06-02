#' Function to squash R check's global variable notes. 
#' 
#' @name zzz
#' 
if (getRversion() >= "2.15.1") {
  
  # What variables are causing issues?
  variables <- c(
    ".", "observation_count", "value", "count", "date_insert", "date_end", 
    "site", "variable", "summary_name", "date_start", "code", "site_name", 
    "process", "validity", "date_start",
    "variables_monitored", "significant", "envirologger_station", "service",
    "envirologger_channel_number", "site_code", "date_local", "date_unix",
    "software", "df_look", "test", "conc", "p", "intercept", "slope", "lower",
    "upper", "summary_source", "auto_correlation", "deseason", "envirologger_sensor_id",
    "year", "data_source", "progress_bar", "intercept_lower", "intercept_upper",
    "envirologger_api_version", "envirologger_sensor_label", "sensor_label",
    "test_current", "n", "current"
  )
  
  # Squash the notes
  utils::globalVariables(variables)
  
}
