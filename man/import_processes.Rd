% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/import_processes.R
\name{import_processes}
\alias{import_processes}
\title{Function to import \code{`processes`} table from a \strong{smonitor} 
database.}
\usage{
import_processes(con, with_sensors = TRUE, with_sites_join = TRUE, tz = "UTC")
}
\arguments{
\item{con}{Database connection to a \strong{smonitor} database.}

\item{with_sensors}{If the \strong{smonitor} data model contains a 
\code{`sensors`} table (or \code{`monitors`} and \code{`deployments`} tables), 
should additional information be joined to the \code{`processes`} return?}

\item{with_sites_join}{Should additional variables from \code{`sites`} be 
joined to the \code{`processes`} table?}

\item{tz}{What time zone should the \code{date_start} and \code{date_end}
variables be represented as?}
}
\value{
Tibble.
}
\description{
Function to import \code{`processes`} table from a \strong{smonitor} 
database.
}
\examples{
\dontrun{

# Import processes from a smonitor database
data_processes <- import_processes(con)

}

}
\seealso{
\code{\link{import_sites}},  \code{\link{import_by_process}}
}
\author{
Stuart K. Grange
}
