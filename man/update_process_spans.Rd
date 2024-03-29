% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/update_process_spans.R
\name{update_process_spans}
\alias{update_process_spans}
\title{Function to calculate \code{date_start}, \code{date_end}, and 
\code{observation_count} for processes and then update the \code{`processes`}
table for an \strong{smonitor} database.}
\usage{
update_process_spans(
  con,
  process = NA,
  by_process = FALSE,
  na.rm = TRUE,
  verbose = FALSE
)
}
\arguments{
\item{con}{Database connection.}

\item{process}{A process vector to update. Default is \code{NA} which will 
cause the database to update all process keys.}

\item{by_process}{Should the database observations spans be calculated 
individually for each process?}

\item{na.rm}{Should missing values (\code{NA}/\code{NULL}) be omitted from 
the aggregation functions?}

\item{verbose}{Should the function give messages?}
}
\value{
Invisible \code{con}.
}
\description{
Function to calculate \code{date_start}, \code{date_end}, and 
\code{observation_count} for processes and then update the \code{`processes`}
table for an \strong{smonitor} database.
}
\author{
Stuart K. Grange
}
