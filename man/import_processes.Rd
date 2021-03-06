% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/import_processes.R
\name{import_processes}
\alias{import_processes}
\title{Function to import \code{`processes`} table from a \strong{smonitor} database.}
\usage{
import_processes(con, tz = "UTC")
}
\arguments{
\item{con}{Database connection to an \strong{smonitor} database.}

\item{tz}{What time zone should the \code{date_start} and \code{date_end}
variables be represented as?}
}
\value{
Tibble.
}
\description{
Function to import \code{`processes`} table from a \strong{smonitor} database.
}
\examples{
\dontrun{

# Import processes from a smonitor database
data_processes <- import_processes(con)

}

}
\author{
Stuart K. Grange
}
