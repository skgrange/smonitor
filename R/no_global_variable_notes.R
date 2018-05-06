#' Squash the global variable notes when building package. 
#' 
if (getRversion() >= "2.15.1") {
  
  # What variables are causing issues?
  variables <- c(
    ".", "observation_count", "value", "count", "date_insert", "date_end", 
    "site", "variable", "summary_name"
  )
  
  # Squash the note
  utils::globalVariables(variables)
  
}
