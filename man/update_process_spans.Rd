% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/update_process_spans.R
\name{update_process_spans}
\alias{update_process_spans}
\title{Function to calculate \code{date_start}, \code{date_end}, and 
\code{observation_count} for processes and then update the \code{`processes`}
table for an \strong{smonitor} database.}
\usage{
update_process_spans(con, tz = "UTC", progress = "text")
}
\arguments{
\item{con}{Database connection.}

\item{tz}{Time-zone to parse the dates to.}

\item{progress}{Type of progress bar to display for the update statements. 
Default is \code{"text"}.}
}
\description{
Function to calculate \code{date_start}, \code{date_end}, and 
\code{observation_count} for processes and then update the \code{`processes`}
table for an \strong{smonitor} database.
}
\author{
Stuart K. Grange
}
\seealso{
\code{\link{calculate_process_spans}}, 
\code{\link{update_site_spans}}
}
