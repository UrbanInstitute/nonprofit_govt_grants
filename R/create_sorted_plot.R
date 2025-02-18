#' Create a Sorted Scatter Plot with Distribution Enhancement
#' @description Creates enhanced scatter plot of values sorted from lowest to highest,
#'   with optional density curve and mean/sd reference lines for better visualization
#'   of heavy-tailed distributions
#' 
#' @param df data.frame. Input dataset
#' @param num_col character scalar. Name of numeric column to plot
#' @param add_density logical. Whether to add density curve overlay. Default TRUE
#' @param add_stats logical. Whether to add mean and sd reference lines. Default TRUE
#' 
#' @return ggplot2 object. Enhanced scatter plot with values sorted from lowest to highest
#' 
#' @examples
#' df <- data.frame(num = rnorm(100))
#' create_sorted_plot(df, "num")
#' 
#' @import dplyr
#' @import ggplot2
#' @export
create_sorted_plot <- function(df, num_col, add_density = TRUE, add_stats = TRUE) {
  # Base plot
  p <- df |>
    dplyr::arrange(.data[[num_col]]) |>
    dplyr::mutate(index = dplyr::row_number()) |>
    ggplot2::ggplot(ggplot2::aes(x = index, y = .data[[num_col]])) +
    ggplot2::geom_point(alpha = 0.6) +  # Add transparency to points
    ggplot2::labs(x = "Index",
                  y = num_col,
                  title = paste("Sorted", num_col, "Values"),
                  subtitle = "With distribution indicators") +
    ggplot2::theme_minimal()
  
  # Add mean and SD reference lines if requested
  if(add_stats) {
    mean_val <- mean(df[[num_col]], na.rm = TRUE)
    sd_val <- sd(df[[num_col]], na.rm = TRUE)
    
    p <- p +
      ggplot2::geom_hline(yintercept = mean_val, 
                          color = "red", 
                          linetype = "dashed") +
      ggplot2::geom_hline(yintercept = c(mean_val - sd_val, mean_val + sd_val), 
                          color = "blue", 
                          linetype = "dotted") +
      ggplot2::annotate("text", 
                        x = max(df$index), 
                        y = mean_val, 
                        label = "Mean", 
                        color = "red",
                        hjust = 1)
  }
  
  # Add density curve on the right if requested
  if(add_density) {
    p <- p +
      ggplot2::geom_density(data = df,
                            ggplot2::aes(x = after_stat(scaled) * max(df$index),
                                         y = .data[[num_col]]),
                            stat = "density",
                            color = "darkgreen",
                            inherit.aes = FALSE)
  }
  
  return(p)
}

# Example usage:
# df <- data.frame(num = rnorm(1000, mean = 0, sd = 1))
# create_sorted_plot(df, "num")
