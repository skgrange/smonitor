% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/update_process_entries.R
\name{update_process_entries}
\alias{update_process_entries}
\title{Function to update a \strong{smonitor} \code{processes} table with the table
generated by \code{\link{summarise_process_entiries}}.}
\usage{
update_process_entries(con, df, verbose = FALSE, progress = FALSE)
}
\arguments{
\item{con}{A \strong{smonitor} database connection.}

\item{df}{A tibble/data frame generated by 
\code{\link{summarise_process_entiries}}.}

\item{verbose}{Should the function give messages?}

\item{progress}{Should a progress bar be displayed?}
}
\value{
Invisible \code{con}.
}
\description{
Function to update a \strong{smonitor} \code{processes} table with the table
generated by \code{\link{summarise_process_entiries}}.
}
\seealso{
\code{\link{summarise_process_entiries}}
}
\author{
Stuart K. Grange
}
