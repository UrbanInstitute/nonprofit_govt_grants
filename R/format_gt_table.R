#' Apply Factsheet Styling to a GT Table
#'
#' Applies the standard factsheet formatting: right border on the first column,
#' bold text on first column and all headers, hidden top border, and 10px row
#' padding.
#'
#' @param gt_table A gt object to style.
#' @param first_col_width Width of the first column in pixels (default 200).
#'
#' @return A styled gt object.
style_factsheet_table <- function(gt_table, first_col_width = 200) {
  gt_table |>
    gt::cols_hide("formatted_percentages") |>
    gt::cols_width(1 ~ gt::px(first_col_width)) |>
    gt::cols_align(align = "left", columns = 1) |>
    gt::tab_style(
      style = gt::cell_borders(
        sides = "right",
        color = "#d2d2d2",
        weight = gt::px(1)
      ),
      locations = gt::cells_body(columns = 1)
    ) |>
    gt::tab_style(
      style = gt::cell_text(weight = "bold"),
      locations = list(gt::cells_body(columns = 1), gt::cells_column_labels())
    ) |>
    gt::tab_options(
      table.border.top.style = "hidden",
      data_row.padding = gt::px(10)
    )
}
