% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/theil_sen_trend_test.R
\name{deseasonalise}
\alias{deseasonalise}
\title{Function to deseasonalise a time series with loess.}
\usage{
deseasonalise(df, variable = "value")
}
\arguments{
\item{df}{Input data frame, containing time series observations.}

\item{variable}{Variable name to test.}
}
\value{
Tibble.
}
\description{
Function to deseasonalise a time series with loess.
}
\seealso{
\code{\link[openair]{TheilSen}}
}
\author{
Stuart K. Grange.
}
