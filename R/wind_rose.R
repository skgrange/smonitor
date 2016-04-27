#' Function to plot wind roses. 
#' 
#' \code{wind_rose} uses \strong{ggplot2} and is a stacked bar chart with
#' polar coordinates. \code{wind_rose} bins wind speed and direction then 
#' aggregates to find counts. 
#' 
#' @param df Data frame. 
#' @param ws Wind speed variable. 
#' @param wd Wind direction variable
#' @param ws_breaks Breaks for wind speed binning. 
#' @param wd_width Width of breaks for wind direction binning. 
#' 
#' @author Stuart K. Grange
#' 
#' @seealso \code{\link{windRose}}
#' 
#' @import ggplot2
#' @importFrom scales percent
#' 
#' @export
wind_rose <- function(df, ws = "ws", wd = "wd", ws_breaks = seq(0, 10, 2), 
                      wd_width = 30) {
  
  df <- threadr::base_df(df)
  
  # Select
  df <- df[, c(ws, wd)]
  
  # No NAs
  df <- df[complete.cases(df), ]
  
  # Bin wind speed and direction
  df[, ws] <- bin_wind_speed(df[, ws], breaks = ws_breaks)
  df[, wd] <- bin_wind_direction(df[, wd], width = wd_width)
  
  # Plot
  plot <- ggplot(df, aes_string(wd, fill = ws)) + 
    geom_bar(aes(y = (..count..) / sum(..count..))) + coord_polar() +
    scale_y_continuous(labels = percent) + ylab("Percentage in direction bin") + 
    xlab("") + guides(fill = guide_legend(title = "Speed bin")) 
  # + scale_fill_brewer(palette = "PuRd")
  
  # Return
  plot
  
}

# 


# No export
bin_wind_direction <- function(x, width, labels = NULL) {
  
  # Catches
  x <- ifelse(x < 0, x + 360, x)
  x <- ifelse(x > 360, x - 360, x)
  
  # labels <- seq(0, 360, width)
  
  x <- cut(x, breaks = seq(0, 360, by = width), include.lowest = TRUE,
           labels = labels)
  
  # Back to degrees after binning
  if (!labels) x <- x * width
  
  # Return
  x
  
}


# No export
bin_wind_speed <- function(x, breaks)
  cut(x, breaks = breaks, include.lowest = TRUE)
