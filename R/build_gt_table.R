#' @title This function creates a table with the GT package for visualizing
#' a table for the factsheet

create_gt_table <- function(df_path, groupby_var, groupby_name) {
  gt_data <- readr::read_csv(df_path)
  gt_table <- gt_data %>%
    gt() %>%
    cols_label(
      "{groupby_var}" := groupby_name,
      mean_months_cash_on_hand_tangibleassets = "Mean",
      median_months_cash_on_hand_tangibleassets = "Median",
      mean_months_cash_on_hand_notangibleassets = "Mean",
      median_months_cash_on_hand_notangibleassets = "Median",
      total_govt_grants = "Total (USD)",
      proportion_govt_grants_20 = "0-20%",
      proportion_govt_grants_40 = "20-40%",
      proportion_govt_grants_60 = "40-60%",
      proportion_govt_grants_80 = "60-80%",
      proportion_govt_grants_100 = "80-100%",
      total_number_nonprofits = "Total"
    ) %>%
    tab_spanner(
      label = html("Months of cash on hand<br>(with tangible assets)"),
      columns = c(mean_months_cash_on_hand_tangibleassets, 
                  median_months_cash_on_hand_tangibleassets)
    ) %>%
    tab_spanner(
      label = html("Months of cash on hand<br>(without tangible assets)"),
      columns = c(mean_months_cash_on_hand_notangibleassets,
                  median_months_cash_on_hand_notangibleassets)
    ) %>%
    tab_spanner(
      label = html("Dollar value of funding<br>from government grants"),
      columns = total_govt_grants
    ) %>%
    tab_spanner(
      label = html("Percentage of nonprofits receiving X%<br>of their money from government grants"),
      columns = c(proportion_govt_grants_20,
                  proportion_govt_grants_40,
                  proportion_govt_grants_60,
                  proportion_govt_grants_80,
                  proportion_govt_grants_100)
    ) %>%
    tab_spanner(
      label = "Number of Nonprofits",
      columns = total_number_nonprofits
    ) %>%
    tab_style(
      style = cell_text(weight = "bold", align = "center"),
      locations = cells_column_labels()
    ) %>%
    tab_style(
      style = cell_text(weight = "bold"),
      locations = cells_column_spanners()
    ) %>%   tab_style(
      style = cell_text(align = "center"),
      locations = cells_body()
    ) %>%
    tab_style(
      style = cell_borders(sides = "left", color = "black", weight = px(1)),
      locations = cells_body()
    ) %>%
    tab_style(
      style = list(
        cell_borders(sides = c("left", "right"), color = "black", weight = px(1))),
      locations =list(
        cells_body(),
        cells_column_labels(),
        cells_column_spanners()
      )
    ) |>
    tab_source_note(source_note = html("<b>Source</b>: National Center for Charitable Statistics 2021 501(c)(3) Charities PZ Scope, available at https://urbaninstitute.github.io/nccs/catalogs/catalog-core.html.")) |>
    opt_horizontal_padding(scale = 3) |>
    opt_vertical_padding(scale = 3)
  return(gt_table)
}