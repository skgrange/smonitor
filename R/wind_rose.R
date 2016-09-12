#' Function to plot wind roses. 
#' 
#' \code{wind_rose} uses \strong{ggplot2} and is a stacked bar chart with
#' polar coordinates. \code{wind_rose} bins wind speed and direction then 
#' aggregates to find counts. 
#' 
#' @param data Data frame. 
#' @param spd Wind speed variable. Default is \code{"ws"}.
#' @param dir Wind direction variable. Default is \code{"wd"}.
#' @param spdres Wind speed resolution. Default is \code{2}.
#' @param dirres Wind direction resolution. Default is \code{30}.
#' @param spdmin Wind speed minimum. Default is \code{2}.
#' @param spdmax Wind speed maximum. Default is \code{20}.
#' @param spdseq Wind speed sequence. An override. 
#' @param palette Colour palette to use. Default is \code{"YlGnBu"}. 
#' @param countmax An adjustment override for the wind speed scale. 
#' @param debug Show debug messages. Use \code{0} or \code{1}, not logicals. 
#' 
#' @author Andy Clifton with a few changes by Stuart K. Grange
#' 
#' @seealso \code{\link{windRose}} \href{http://stackoverflow.com/questions/17266780/wind-rose-with-ggplot-r}{stackoverflow.com}
#' 
#' @import ggplot2
#' @import scales
#' 
#' @export
wind_rose <- function(data,
                      spd = "ws",
                      dir = "wd",
                      spdres = 2,
                      dirres = 30,
                      spdmin = 2,
                      spdmax = 20,
                      spdseq = NULL,
                      palette = "YlGnBu",
                      countmax = NA,
                      debug = 0) {

  # Look to see what data was passed in to the function
  if (is.numeric(spd) & is.numeric(dir)){
    
    # assume that we've been given vectors of the speed and direction vectors
    data <- data.frame(spd = spd, dir = dir)
    spd = "spd"
    dir = "dir"
    
  } else if (exists("data")){
    
    # Assume that we've been given a data frame, and the name of the speed
    # and direction columns. This is the format we want for later use.
    
  }

  # Tidy up input data
  n.in <- NROW(data)
  dnu <- (is.na(data[[spd]]) | is.na(data[[dir]]))
  data[[spd]][dnu] <- NA
  data[[dir]][dnu] <- NA

  # figure out the wind speed bins
  if (missing(spdseq)) {
    
    spdseq <- seq(spdmin, spdmax, spdres)
    
  } else {
    
    if (debug > 0) cat("Using custom speed bins \n")
    
  }
  # get some information about the number of bins, etc.
  n.spd.seq <- length(spdseq)
  n.colors.in.range <- n.spd.seq - 1

  # create the color map
  spd.colors <- colorRampPalette(RColorBrewer::brewer.pal(min(max(3,
                                                    n.colors.in.range),
                                                min(9,
                                                    n.colors.in.range)),
                                            palette))(n.colors.in.range)

  if (max(data[[spd]],na.rm = TRUE) > spdmax) {
    
    spd.breaks <- c(spdseq,
                    max(data[[spd]],na.rm = TRUE))
    spd.labels <- c(paste(c(spdseq[1:n.spd.seq-1]),
                          '-',
                          c(spdseq[2:n.spd.seq])),
                    paste(spdmax,
                          "-",
                          max(data[[spd]],na.rm = TRUE)))
    spd.colors <- c(spd.colors, "grey50")
    
  } else {
    
    spd.breaks <- spdseq
    spd.labels <- paste(c(spdseq[1:n.spd.seq-1]),
                        '-',
                        c(spdseq[2:n.spd.seq]))
  }
  
  data$spd.binned <- cut(x = data[[spd]],
                         breaks = spd.breaks,
                         labels = spd.labels,
                         ordered_result = TRUE)

  # figure out the wind direction bins
  dir.breaks <- c(-dirres/2,
                  seq(dirres/2, 360-dirres/2, by = dirres),
                  360+dirres/2)
  
  dir.labels <- c(paste(360-dirres/2,"-",dirres/2),
                  paste(seq(dirres/2, 360-3*dirres/2, by = dirres),
                        "-",
                        seq(3*dirres/2, 360-dirres/2, by = dirres)),
                  paste(360-dirres/2,"-",dirres/2))
  
  # assign each wind direction to a bin
  dir.binned <- cut(data[[dir]],
                    breaks = dir.breaks,
                    ordered_result = TRUE)
  
  levels(dir.binned) <- dir.labels
  data$dir.binned <- dir.binned

  # Run debug if required
  if (debug > 0) {
    
    cat(dir.breaks,"\n")
    cat(dir.labels,"\n")
    cat(levels(dir.binned),"\n")

  }

  # create the plot
  p.windrose <- ggplot(data = data, aes(x = dir.binned, fill = spd.binned)) +
    geom_bar() + scale_x_discrete(drop = FALSE, labels = waiver()) +
    coord_polar(start = -((dirres / 2) / 360) * 2 * pi) +
    scale_fill_manual(name = "Wind Speed (m/s)", values = spd.colors, 
                      drop = FALSE) + theme(axis.title.x = element_blank()) + 
    theme_minimal()

  # adjust axes if required
  if (!is.na(countmax)) {
    
    p.windrose <- p.windrose + ylim(c(0, countmax))
    
  }

  # print the plot
  print(p.windrose)

  # return the handle to the wind rose
  return(p.windrose)
  
}


# wind_rose <- function(df, ws = "ws", wd = "wd", ws_bin = 1,
#                       wd_width = 10) {
# 
#   df <- threadr::base_df(df)
# 
#   # Select
#   df <- df[, c(ws, wd)]
# 
#   # No NAs
#   df <- df[complete.cases(df), ]
# 
#   # Get max wind speed
#   ws_max <- max(df[, ws])
#   ws_max <- ceiling(ws_max)
# 
#   #
#   ws_breaks <- seq(2, ws_max, ws_bin)
# 
#   # Bin wind speed and direction
#   df[, "ws_bin"] <- bin_wind_speed(df[, ws], breaks = ws_breaks)
#   df[, "wd_bin"] <- bin_wind_direction(df[, wd], width = wd_width)
# 
#   # Plot
#   plot <- ggplot(df, aes(wd_bin, fill = ws_bin)) +
#     geom_bar(aes(y = (..count..) / sum(..count..))) + coord_polar() +
#     scale_y_continuous(labels = percent) + ylab("Percentage in wind direction bin") +
#     xlab("") + guides(fill = guide_legend(title = "Wind speed bin"))
#   # + scale_fill_brewer(palette = "PuRd")
# 
#   # Return
#   plot
# 
# }
# 
#
# 
# # No export
# bin_wind_direction <- function(x, width) {
# 
#   # Catches
#   x <- ifelse(x < 0, x + 360, x)
#   x <- ifelse(x > 360, x - 360, x)
#   
#   breaks <- c(-width/2,
#                   seq(width/2, 360-width/2, by = width),
#                   360+width/2)  
# 
#   labels <- c(paste(360-width/2,"-",width/2),
#                   paste(seq(width/2, 360-3*width/2, by = width),
#                         "-",
#                         seq(3*width/2, 360-width/2, by = width)),
#                   paste(360-width/2,"-",width/2))
# 
#   x <- cut(x, breaks = breaks, include.lowest = TRUE,
#            labels = NULL)
#   
#   levels(x) <- labels
# 
#   # Back to degrees after binning
#   # if (!is.null(labels)) x <- x * width
# 
#   # Return
#   x
# 
# }
# 
# 
# # No export
# bin_wind_speed <- function(x, breaks)
#   cut(x, breaks = breaks, include.lowest = TRUE)
