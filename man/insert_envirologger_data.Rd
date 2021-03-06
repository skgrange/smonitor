% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/insert_envirologger_data.R
\name{insert_envirologger_data}
\alias{insert_envirologger_data}
\title{Function to get observations from the Envirologger API and insert them into a 
\strong{smonitor} database.}
\usage{
insert_envirologger_data(
  con,
  user,
  key,
  station,
  start,
  end = NA,
  api_version = 3.5,
  verbose = FALSE
)
}
\arguments{
\item{con}{Database connection.}

\item{user}{An Envirologger API user-name.}

\item{key}{An Envirologger API key for \code{user}.}

\item{station}{A vector of station codes to download.}

\item{start}{Start date to download and insert.}

\item{end}{End date to download and insert.}

\item{api_version}{What API version is in use? Use \code{3.5} for different 
joining logic which uses \code{channel} and \code{sensor_label} variables.}

\item{verbose}{Should the funciton give messages?}
}
\value{
Invisible \code{con}.
}
\description{
Site-variable combinations must to be present in the database's process table,
otherwise they will be silently filtered and not be inserted. New, downloaded
observations will take priority over those stored in the database and old 
observations are deleted with this function.
}
\author{
Stuart K. Grange
}
