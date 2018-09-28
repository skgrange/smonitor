#' Function to squash R check's global variable notes. 
#' 
if (getRversion() >= "2.15.1") {
  
  # What variables are causing issues?
  variables <- c(
    "date_start", "code", "site_name", "process", "validity", "date_start",
    "variables_monitored", "significant", "envirologger_station", "service",
    "envirologger_channel_number", "site_code", "date_local", "date_unix",
    "software"
  )
  
  # Squash the notes
  utils::globalVariables(variables)
  
}