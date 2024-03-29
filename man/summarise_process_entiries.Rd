% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/summarise_process_entiries.R
\name{summarise_process_entiries}
\alias{summarise_process_entiries}
\title{Function to calculate process summaries in a \strong{smonitor} database.}
\usage{
summarise_process_entiries(
  con,
  process,
  batch_size = NA,
  tz = "UTC",
  progress = FALSE
)
}
\arguments{
\item{con}{A \strong{smonitor} database connection.}

\item{process}{A vector of processes.}

\item{batch_size}{Number of processes to summarise per batch. This can be 
useful when large numbers of processes are to be summarised.}

\item{tz}{Time zone for the dates to be parsed into.}

\item{progress}{Should a progress bar be displayed?}
}
\value{
Tibble.
}
\description{
\code{summarise_process_entiries} will calculate \code{date_start}, 
\code{date_end}, \code{n_all}, and \code{n} variables that can be used with 
\code{\link{update_process_entries}} to update a \code{processes} table.
}
\seealso{
\code{\link{update_process_entries}}
}
\author{
Stuart K. Grange
}
