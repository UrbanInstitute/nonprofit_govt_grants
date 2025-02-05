# Script Header
# Title: Federal Funding Freeze Blog Post
# Description: This script contains the code to replicate tables pulled for a
# blog post containing details on the how badly nonprofits can be affected by 
# the federal funding freeze

# Setup
dir.create("data")
dir.create("data/raw")
dir.create("data/processed")
dir.create("R")

# Packages
library(rio)
library(data.table)
library(dtplyr)
library(tidyverse)
library(tidyr)
library(purrr)
library(tidylog)
library(usdata)
states <- as.character(usdata::state_stats$abbr)
# Scripts
source("R/proportion_govt_grant.R")
source("R/derive_ein2.R")

# (1) - Download raw data

# 2022 Efile data for Part 08
download.file(
  "https://nccs-efile.s3.us-east-1.amazonaws.com/parsed/F9-P08-T00-REVENUE-2022.csv",
  "data/raw/efile_p08_2022.csv"
)
# 2022 Efile data for Part 09
download.file(
  "https://nccs-efile.s3.us-east-1.amazonaws.com/parsed/F9-P09-T00-EXPENSES-2022.csv",
  "data/raw/efile_p09_2022.csv"
)

# (2) -  Read in files, with relevant columns filtered

# (2.1) - Columns to read in

efile_p08_cols <- c(
  "ORG_EIN",
  "TAX_YEAR",
  "F9_08_REV_CONTR_GOVT_GRANT",
  "F9_08_REV_TOT_TOT"
)

efile_p09_cols <- c(
  "ORG_EIN",
  "TAX_YEAR",
  "F9_09_EXP_TOT_TOT"
)

efile_num_cols <- c(
  "F9_08_REV_CONTR_GOVT_GRANT",
  "F9_08_REV_TOT_TOT"
)

# (2.2) - Read in data

# Efile
efile_p08_2022 <- data.table::fread("data/raw/efile_p08_2022.csv", 
                                    select = efile_p08_cols)
efile_p09_2022 <- data.table::fread("data/raw/efile_p09_2022.csv",
                                    select = efile_p09_cols)

# (3) - Wrangle Data

# Efile p08
efile_p08_sample <- efile_p08_2022 |>
  dplyr::select(dplyr::all_of(efile_p08_cols)) |>
  dplyr::filter(TAX_YEAR == 2022) |>
  dplyr::mutate(dplyr::across(dplyr::all_of(efile_num_cols), as.numeric)) |>
  dplyr::mutate(
    EIN2 = purrr::pmap_chr(
      list(ORG_EIN),
      derive_ein2,
      .progress = TRUE
    )
  )

# Efile p09
efile_p09_sample <- efile_p09_2022 |>
  dplyr::select(dplyr::all_of(efile_p09_cols)) |>
  dplyr::filter(TAX_YEAR == 2022) |>
  dplyr::mutate(F9_09_EXP_TOT_TOT = as.numeric(F9_09_EXP_TOT_TOT)) |>
  dplyr::mutate(
    EIN2 = purrr::pmap_chr(
      list(ORG_EIN),
      derive_ein2,
      .progress = TRUE
    )
  )

# (3.2) - Merge Data
efile_merge <- efile_p08_sample |>
  tidylog::left_join(
    bmf_2025,
    by = c("EIN2" = "EIN2")
  ) |>
  tidylog::left_join(
    efile_p09_sample,
    by = c("EIN2" = "EIN2")
  ) 

# (3.3) Filter data
efile_merge <- efile_merge |>
  dplyr::select(
    EIN2,
    F9_08_REV_CONTR_GOVT_GRANT,
    F9_08_REV_TOT_TOT,
    F9_09_EXP_TOT_TOT,
    STATE,
    SUBSECTOR
  )

# (3.4) Create expense categories
efile_merge <- efile_merge |>
  dplyr::mutate(
    expense_category = dplyr::case_when(
      F9_09_EXP_TOT_TOT < 100000 ~ "Less than $100K",
      F9_09_EXP_TOT_TOT >= 100000 &
        F9_09_EXP_TOT_TOT < 500000 ~ "Between $100K and $499K",
      F9_09_EXP_TOT_TOT >= 500000 &
        F9_09_EXP_TOT_TOT < 1000000 ~ "Between $500K and $999K",
      F9_09_EXP_TOT_TOT >= 1000000 &
        F9_09_EXP_TOT_TOT < 5000000 ~ "Between $1M and $4.99M",
      F9_09_EXP_TOT_TOT >= 5000000 &
        F9_09_EXP_TOT_TOT < 10000000 ~ "Between $5M and $9.99M",
      F9_09_EXP_TOT_TOT >= 10000000 ~ "Greater than $10M",
      .default = "Less than $100K"
    )
  )

# (3.5) Rename columns and convert datatypes
efile_merge <- efile_merge |>
  dplyr::rename(
    total_expenses = F9_09_EXP_TOT_TOT,
    total_revenue = F9_08_REV_TOT_TOT,
    govt_grant = F9_08_REV_CONTR_GOVT_GRANT,
    state = STATE,
    subsector = SUBSECTOR,
    ein = EIN2
  ) |>
  dplyr::mutate(
    total_expenses = as.numeric(total_expenses),
    total_revenue = as.numeric(total_revenue),
    govt_grant = as.numeric(govt_grant)
  ) |>
  tidyr::replace_na(list(govt_grant = 0, total_revenue = 0))

# (4) - Government Grants

gvt_grnt <- efile_merge |>
  dplyr::mutate(
    proportion_govt_grant = purrr::pmap_dbl(
      list(govt_grant, total_revenue),
      proportion_govt_grant,
      .progress = TRUE
    )
  )

gvt_grnt_bysubsector <- gvt_grnt |>
  dplyr::group_by(state, subsector) |>
  dplyr::summarise(
    mean_proportion_govt_grant = round(mean(proportion_govt_grant, na.rm = TRUE), 2),
    median_proportion_govt_grant = round( median(proportion_govt_grant, na.rm = TRUE), 2),
    median_govt_grant = round(median(govt_grant, na.rm = TRUE), 2),
    mean_govert_grant = round(mean(govt_grant, na.rm = TRUE), 2),
    number_nonprofits = dplyr::n_distinct(ein)
  ) |>
  dplyr::filter(state %in% states)

gvt_grnt_byexpense <- gvt_grnt |>
  dplyr::group_by(state, expense_category) |>
  dplyr::summarise(
    mean_proportion_govt_grant = round(mean(proportion_govt_grant, na.rm = TRUE), 2),
    median_proportion_govt_grant = round(median(proportion_govt_grant, na.rm = TRUE), 2),
    median_govt_grant = round(median(govt_grant, na.rm = TRUE), 2),
    mean_govt_grant = round(mean(govt_grant, na.rm = TRUE), 2),
    number_nonprofits = dplyr::n_distinct(ein)
  ) |>
  dplyr::filter(state %in% states)

# (5) - Save data
rio::export(gvt_grnt_bysubsector, "data/processed/govt_grant_bysubsector.csv")
rio::export(gvt_grnt_byexpense, "data/processed/govt_grant_byexpense.csv")


gvt_grnt |>
  dplyr::mutate(
    expense_category = dplyr::case_when(
      total_expenses < 50000 ~ "Less than $50k",
      total_expenses >= 50000 &
        total_expenses < 100000 ~ "Between $50K and $99K",
      total_expenses >= 100000 &
        total_expenses < 500000 ~ "Between $100K and $499K",
      total_expenses >= 500000 &
        total_expenses < 1000000 ~ "Between $500K and $999K",
      total_expenses >= 1000000 &
        total_expenses < 5000000 ~ "Between $1M and $4.99M",
      total_expenses >= 5000000 &
        total_expenses < 10000000 ~ "Between $5M and $9.99M",
      total_expenses >= 10000000 ~ "Greater than $10M",
      .default = "No expense data available"
    )
  ) |>
  dplyr::mutate(
    received_government_grant = ifelse(govt_grant > 0, 1, 0)
  ) |>
  dplyr::group_by(expense_category) |>
  dplyr::summarise(
    number_received_government_grant = round(sum(received_government_grant, na.rm = TRUE), 2),
    number_nonprofits = dplyr::n_distinct(ein)
  ) |> 
  dplyr::mutate(
    proportion_received_government_grant = round(number_received_government_grant / number_nonprofits, 2)
  ) |> kableExtra::kable() |> kableExtra::kable_styling()
