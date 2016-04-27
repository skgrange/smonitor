#' Function to plot polar plots. 
#' 
#' \code{polar_plot} is slow due to the combination of using \strong{ggplot}'s 
#' \code{geom_tile} and \code{coord_polar} at the same time. 
#' 
#' @author Stuart K. Grange
#' 
#' @import ggplot2
#' 
#' @export
polar_plot <- function(df, variable, ws = "ws", wd = "wd", polar = TRUE, 
                       contour = FALSE) {
  
  df <- threadr::base_df(df)
  
  # Bin wind direction
  # df[, wd] <- bin_wind_direction(df[, wd], 30, labels = FALSE)
  # df[, wd] <- df[, wd] * 30

  plot <- ggplot(df, aes_string(wd, ws, z = variable)) + 
    geom_tile(aes_string(fill = variable)) + 
    scale_fill_gradient(low = "#1cb4d4", high = "#d631a0") 
  # + theme(panel.grid.major = element_line(colour = "black"))
  # scale_fill_gradient(low = "red", high = "white")
  
  if (contour) plot <- plot + stat_contour(colour = "black")
  if (polar) plot <- plot + coord_polar()
  
  # Return
  plot
  
}
