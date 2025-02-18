#' @title This function creates a table with the GT package for visualizing
#' a table for the factsheet

build_gt_table <- function(df_path, groupby_var, groupby_name) {
  # Wrangle Data
  gt_data <- readr::read_csv(df_path)
  gt_data <- gt_data |>
    dplyr::select(!mean_months_cash_on_hand_tangibleassets) |>
    dplyr::select(!mean_months_cash_on_hand_notangibleassets)
  if (groupby_var == "SUBSECTOR") {
    gt_data <- gt_data |>
      dplyr::mutate(SUBSECTOR = dplyr::case_when(
        SUBSECTOR == "ART" ~ "Arts, culture, and humanities",
        SUBSECTOR == "EDU" ~ "Education (minus universities)",
        SUBSECTOR == "ENV" ~ "Environment and animals",
        SUBSECTOR == "HEL" ~ "Health (minus hospitals)",
        SUBSECTOR == "HMS" ~ "Human services",
        SUBSECTOR == "HOS" ~ "Hospitals",
        SUBSECTOR == "IFA" ~ "International, foreign affairs",
        SUBSECTOR == "MMB" ~ "Mutual/membership benefit",
        SUBSECTOR == "PSB" ~ "Public, societal benefit",
        SUBSECTOR == "REL" ~ "Religion",
        SUBSECTOR == "UNI" ~ "Universities",
        SUBSECTOR == "UNU" ~ "Unknown/unclassified",
        .default = "Unknown/unclassified"
      ))
  }
  gt_data_1 <- gt_data |>
    dplyr::select(
      !!sym(groupby_var),
      median_months_cash_on_hand_tangibleassets,
      median_months_cash_on_hand_notangibleassets,
      total_govt_grants
    )
  gt_data_2 <- gt_data |>
    dplyr::select(
      !!sym(groupby_var),
      proportion_govt_grants_20,
      proportion_govt_grants_40,
      proportion_govt_grants_60,
      proportion_govt_grants_80,
      proportion_govt_grants_100,
      total_number_nonprofits
    )
  
  # Create tables
  gt_table_1 <- gt_data_1 |>
    gt() |>
    cols_label(
      "{groupby_var}" := groupby_name,
      median_months_cash_on_hand_tangibleassets = "Median",
      median_months_cash_on_hand_notangibleassets = "Median",
      total_govt_grants = "Total (USD)"
    ) |>
    tab_spanner(
      label = html("Months of cash on hand (with tangible assets)"),
      columns = c(median_months_cash_on_hand_tangibleassets)
    ) |>
    tab_spanner(
      label = html("Months of cash on hand (without tangible assets)"),
      columns = c(median_months_cash_on_hand_notangibleassets)
    ) |>
    tab_spanner(
      label = html("Dollar value of funding from government grants"),
      columns = total_govt_grants
    ) |>
    tab_style(
      style = cell_text(weight = "bold", align = "center"),
      locations = cells_column_labels()
    ) %>%
    tab_style(
      style = cell_text(weight = "bold", align = "left"),
      locations = cells_column_spanners()
    ) %>%   tab_style(
      style = cell_text(align = "center"),
      locations = cells_body()
    ) |>
    tab_options(data_row.padding.horizontal = px(20))
  
  gt_table_2 <- gt_data_2 |>
    gt() |>
    cols_label(
      "{groupby_var}" := groupby_name,
      proportion_govt_grants_20 = "0-20%",
      proportion_govt_grants_40 = "20-40%",
      proportion_govt_grants_60 = "40-60%",
      proportion_govt_grants_80 = "60-80%",
      proportion_govt_grants_100 = "80-100%",
      total_number_nonprofits = "Total"
    ) |>
    tab_spanner(
      label = html("Percentage of nonprofits receiving X% of their money from government grants"),
      columns = c(proportion_govt_grants_20,
                  proportion_govt_grants_40,
                  proportion_govt_grants_60,
                  proportion_govt_grants_80,
                  proportion_govt_grants_100)
    ) |>
    tab_spanner(
      label = "Number of Nonprofits",
      columns = total_number_nonprofits
    ) |>
    tab_style(
      style = cell_text(weight = "bold", align = "center"),
      locations = cells_column_labels()
    ) %>%
    tab_style(
      style = cell_text(weight = "bold", align = "left"),
      locations = cells_column_spanners()
    ) %>%   tab_style(
      style = cell_text(align = "center"),
      locations = cells_body()
    ) |>
    tab_options(data_row.padding.horizontal = px(20))
  
  
  # gt_table <- gt_data %>%
  #   gt() |>
  #   cols_label(
  #     "{groupby_var}" := groupby_name,
  #     median_months_cash_on_hand_tangibleassets = "Median",
  #     median_months_cash_on_hand_notangibleassets = "Median",
  #     total_govt_grants = "Total (USD)",
  #     proportion_govt_grants_20 = "0-20%",
  #     proportion_govt_grants_40 = "20-40%",
  #     proportion_govt_grants_60 = "40-60%",
  #     proportion_govt_grants_80 = "60-80%",
  #     proportion_govt_grants_100 = "80-100%",
  #     total_number_nonprofits = "Total"
  #   ) |>
  #   tab_spanner(
  #     label = html("Months of cash on hand (with tangible assets)"),
  #     columns = c(median_months_cash_on_hand_tangibleassets)
  #   ) %>%
  #   tab_spanner(
  #     label = html("Months of cash on hand (without tangible assets)"),
  #     columns = c(median_months_cash_on_hand_notangibleassets)
  #   ) %>%
  #   tab_spanner(
  #     label = html("Dollar value of funding from government grants"),
  #     columns = total_govt_grants
  #   ) %>%
  #   tab_spanner(
  #     label = html("Percentage of nonprofits receiving X% of their money from government grants"),
  #     columns = c(proportion_govt_grants_20,
  #                 proportion_govt_grants_40,
  #                 proportion_govt_grants_60,
  #                 proportion_govt_grants_80,
  #                 proportion_govt_grants_100)
  #   ) %>%
  #   tab_spanner(
  #     label = "Number of Nonprofits",
  #     columns = total_number_nonprofits
  #   ) %>%
  #   tab_style(
  #     style = cell_text(weight = "bold", align = "center"),
  #     locations = cells_column_labels()
  #   ) %>%
  #   tab_style(
  #     style = cell_text(weight = "bold", align = "left"),
  #     locations = cells_column_spanners()
  #   ) %>%   tab_style(
  #     style = cell_text(align = "center"),
  #     locations = cells_body()
  #   ) |>
  #   tab_options(data_row.padding.horizontal = px(20))
  return(list(gt_table_1, gt_table_2))
}