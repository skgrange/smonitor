% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/check_process_keys.R
\name{check_process_keys}
\alias{check_process_keys}
\title{Function to check if a vector of process keys are allocated in the processes
table in an \strong{smonitor} database.}
\usage{
check_process_keys(con, process)
}
\arguments{
\item{con}{Database connection.}

\item{process}{Process vector.}
}
\value{
Invisible, an error if the test fails.
}
\description{
Function to check if a vector of process keys are allocated in the processes
table in an \strong{smonitor} database.
}
\author{
Stuart K. Grange
}
