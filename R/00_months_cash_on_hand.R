# Script Header
# Title: Federal Funding Freeze Blog Post
# Description: This script contains the code to replicate tables pulled for a
# blog post containing details on the how badly nonprofits can be affected by 
# the federal funding freeze. We are using data for the 2021 tax year.

# Create necessary folders
dir.create("data")
dir.create("data/raw")
dir.create("data/processed")
dir.create("R")

# Packages and default datasets
library(rio)
library(data.table)
library(dtplyr)
library(tidyverse)
library(tidyr)
library(purrr)
library(tidylog)
library(usdata)
states <- as.character(usdata::state_stats$abbr) # 51 states

# Helper Scripts
source("R/spending_on_hand.R")
source("R/derive_ein2.R")

# (1) - Download raw data

# SOI for 2022 Calendar Year
download.file("https://www.irs.gov/pub/irs-soi/22eoextract990.xlsx", 
              "data/raw/soi22_raw.xlsx")

# SOI for 2021 Calendar Year
download.file("https://www.irs.gov/pub/irs-soi/23eoextract990.xlsx", 
              "data/raw/soi21_raw.xlsx")

# Unified BMF
download.file("https://nccsdata.s3.amazonaws.com/harmonized/bmf/unified/BMF_UNIFIED_V1.1.csv",
              "data/raw/unified_bmf.csv")
# Most recent BMF - 2025
download.file("https://nccsdata.s3.us-east-1.amazonaws.com/raw/bmf/2025-01-BMF.csv",
              "data/raw/bmf_2025.csv")

# (2) -  Read in .xlsx files and save to csv, with relevant columns filtered

# (2.1) - Columns to read in

soi_23_cols <- c(
  "ein", 
  "tax_pd",
  "unrstrctnetasstsend", # unrestricted net assets
  "lndbldgsequipend", # land, buildings, and equipment
  "txexmptbndsend", # tax-exempt bond liabilities
  "secrdmrtgsend", # secured mortgages and notes payable
  "unsecurednotesend", # unsecured mortgages and notes payable
  "totfuncexpns", # total functional expenses - same as total expenses
  "deprcatndepletn" # depreciation
)

soi_22_cols <- c(
  "EIN", 
  "tax_pd",
  "unrstrctnetasstsend", # unrestricted net assets
  "lndbldgsequipend", # land, buildings, and equipment
  "txexmptbndsend", # tax-exempt bond liabilities
  "secrdmrtgsend", # secured mortgages and notes payable
  "unsecurednotesend", # unsecured mortgages and notes payable
  "totfuncexpns", # total functional expenses - same as total expenses
  "deprcatndepletn" # depreciation
)

soi_numeric_cols <- c(
  "unrstrctnetasstsend",
  "lndbldgsequipend",
  "txexmptbndsend",
  "secrdmrtgsend",
  "unsecurednotesend",
  "totfuncexpns",
  "deprcatndepletn"
)

# SOI Files
soi_2023 <- readxl::read_xlsx("data/raw/23eoextract990.xlsx")
rio::export(soi_2023, "data/raw/soi23_raw.csv")
soi_2022 <- readxl::read_xlsx("data/raw/22eoextract990.xlsx")
rio::export(soi_2022, "data/raw/soi22_raw.csv")

soi_23 <- data.table::fread("data/raw/soi23_raw.csv", select = soi_23_cols)
soi_22 <- data.table::fread("data/raw/soi22_raw.csv", select = soi_22_cols)

# Unified BMF
unified_bmf <- data.table::fread(
  "~/Urban/NCCS/nccstools/harmonize/data/unified/unified_bmf/BMF_UNIFIED_V1.1.csv",
  select = c("EIN2", "NTEEV2", "CENSUS_STATE_ABBR")
)

# 2025 BMF
bmf_2025 <- data.table::fread("data/raw/bmf_2025.csv", select = c("EIN", 
                                                                  "STATE", 
                                                                  "NTEE_CD"))

# (3) - Wrangle Data

# (3.1) - Rename columns
soi_22 <- soi_22 |>
  dplyr::rename(ein = EIN)

# (3.2) - Combine SOI datasets into sample
soi_sample <- data.table::rbindlist(list(soi_22, soi_23))

# (3.3) - Wrangle datatypes
soi_sample <- soi_sample |>
  dplyr::mutate(across(dplyr::all_of(soi_numeric_cols), as.numeric))

# (3.5) - Process columns

# SOI
soi_sample <- soi_sample |>
  dplyr::mutate(tax_year = as.character(substr(tax_pd, 1, 4))) |> # Tax Year
  dplyr::mutate(EIN2 = purrr::pmap_chr(
    list(ein),
    derive_ein2,
    .progress = TRUE
  )) |> # EIN2
  dplyr::filter(tax_year == "2022")

# BMF 2025
bmf_2025 <- bmf_2025 |>
  dplyr::mutate(EIN2 = purrr::pmap_chr(
    list(EIN),
    derive_ein2,
    .progress = TRUE
  )) # EIN2

bmf_2025[, SUBSECTOR := data.table::fcase(
  stringr::str_starts(NTEE_CD, "A"), "ART",
  stringr::str_starts(NTEE_CD, "B4") | stringr::str_starts(NTEE_CD, "B5"), "EDU",
  stringr::str_starts(NTEE_CD, "B"), "UNI",
  stringr::str_starts(NTEE_CD, "C") | stringr::str_starts(NTEE_CD, "D"), "ENV",
  stringr::str_starts(NTEE_CD, "E2"), "HOS",
  stringr::str_starts(NTEE_CD, "E") | 
    stringr::str_starts(NTEE_CD, "F") | 
    stringr::str_starts(NTEE_CD, "G") | 
    stringr::str_starts(NTEE_CD, "H"), "HEL",
  stringr::str_starts(NTEE_CD, "I") | 
    stringr::str_starts(NTEE_CD, "J") | 
    stringr::str_starts(NTEE_CD, "K") | 
    stringr::str_starts(NTEE_CD, "L") | 
    stringr::str_starts(NTEE_CD, "M") | 
    stringr::str_starts(NTEE_CD, "N") | 
    stringr::str_starts(NTEE_CD, "O") | 
    stringr::str_starts(NTEE_CD, "P"), "HMS",
  stringr::str_starts(NTEE_CD, "Q"), "IFA",
  stringr::str_starts(NTEE_CD, "R") | 
    stringr::str_starts(NTEE_CD, "S") | 
    stringr::str_starts(NTEE_CD, "T") | 
    stringr::str_starts(NTEE_CD, "U") | 
    stringr::str_starts(NTEE_CD, "V") | 
    stringr::str_starts(NTEE_CD, "W"), "PSB",
  stringr::str_starts(NTEE_CD, "X"), "REL",
  stringr::str_starts(NTEE_CD, "Y"), "MMB",
  default = "UNU"
)] # Subsector

# Replace all NAs with zero
soi_sample <- soi_sample |> 
  dplyr::mutate(across(dplyr::all_of(soi_numeric_cols), ~ tidyr::replace_na(., 0)))

# (3.4) - Summary
summary(soi_sample)

# (4) Compute months of cash on hand

soi_sample <- soi_sample |>
  dplyr::mutate(
    tot_mrtgntspybl = secrdmrtgsend + unsecurednotesend
  ) |> # Tota mortgage and notes payable
  dplyr::mutate(months_cash_on_hand = purrr::pmap_dbl(
    list(
      unrstrctnetasstsend,
      lndbldgsequipend,
      txexmptbndsend,
      tot_mrtgntspybl,
      totfuncexpns,
      deprcatndepletn
    ),
    spending_on_hand,
    .progress = TRUE
  ))

# (5) - Merge with BMF data

soi_bmf_merge <- soi_sample |>
  tidylog::left_join(
    bmf_2025,
    by = c("EIN2" = "EIN2")
  )

# (6) - Post processing data (mcoh: months of cash on hand)

mcoh <- soi_bmf_merge |>
  dplyr::select(EIN2, months_cash_on_hand, SUBSECTOR, totfuncexpns, STATE) |>
  dplyr::rename(
    ein = EIN2,
    subsector = SUBSECTOR,
    total_expenses = totfuncexpns,
    state = STATE
  )
# By state and subsector
mcoh_bysubsector <- mcoh |>
  dplyr::group_by(state, subsector) |>
  dplyr::summarise(
    median_months_cash_on_hand = round(median(months_cash_on_hand, na.rm = TRUE), 2),
    number_nonprofits = dplyr::n_distinct(ein)
  ) |>
  dplyr::filter(state %in% states)
# By state and expense category
mcoh_byexpense <- mcoh |>
  dplyr::mutate(
    expense_category = dplyr::case_when(
      total_expenses < 100000 ~ "Less than $100K",
      total_expenses >= 100000 &
        total_expenses < 500000 ~ "Between $100K and $499K",
      total_expenses >= 500000 &
        total_expenses < 1000000 ~ "Between $500K and $999K",
      total_expenses >= 1000000 &
        total_expenses < 5000000 ~ "Between $1M and $4.99M",
      total_expenses >= 5000000 &
        total_expenses < 10000000 ~ "Between $5M and $9.99M",
      total_expenses >= 10000000 ~ "Greater than $10M",
      .default = "No Expenses Provided"
    )
  ) |>
  dplyr::group_by(state, expense_category) |>
  dplyr::summarise(
    median_months_cash_on_hand = round(median(months_cash_on_hand, na.rm = TRUE), 2),
    number_nonprofits = dplyr::n_distinct(ein)
  ) |>
  dplyr::filter(state %in% states)

# (8) - Save data

rio::export(mcoh_bysubsector, "data/processed/mcoh_bysubsector.csv")
rio::export(mcoh_byexpense, "data/processed/mcoh_byexpense.csv")

# efile data

download.file("https://nccs-efile.s3.us-east-1.amazonaws.com/parsed/F9-P08-T00-REVENUE-2022.csv",
              "data/raw/efile_p08_2022.csv")
download.file(
  "https://nccs-efile.s3.us-east-1.amazonaws.com/parsed/F9-P09-T00-EXPENSES-2022.csv",
  "data/raw/efile_p09_2022.csv"
)


efile_p08_2022 <- data.table::fread("data/raw/efile_p08_2022.csv")
efile_p09_2022 <- data.table::fread("data/raw/efile_p09_2022.csv")


efile_cols <- c(
  "ORG_EIN",
  "TAX_YEAR",
  "F9_08_REV_CONTR_GOVT_GRANT",
  "F9_08_REV_TOT_TOT"
)

efile_num_cols <- c(
  "F9_08_REV_CONTR_GOVT_GRANT",
  "F9_08_REV_TOT_TOT"
)

efile_p09_cols <- c(
  "ORG_EIN",
  "TAX_YEAR",
  "F9_09_EXP_TOT_TOT"
)

efile_sample <- efile_2022 |>
  dplyr::select(dplyr::all_of(efile_cols)) |>
  dplyr::filter(TAX_YEAR == 2022) |>
  dplyr::mutate(dplyr::across(dplyr::all_of(efile_num_cols), as.numeric)) |>
  dplyr::mutate(
    EIN2 = purrr::pmap_chr(
      list(ORG_EIN),
      derive_ein2,
      .progress = TRUE
    )
  )

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


efile_merge <- efile_sample |>
  tidylog::left_join(
    bmf_2025,
    by = c("EIN2" = "EIN2")
  ) |>
  tidylog::left_join(
    efile_p09_sample,
    by = c("EIN2" = "EIN2")
  ) 

efile_merge <- efile_merge |>
  dplyr::select(
    EIN2,
    F9_08_REV_CONTR_GOVT_GRANT,
    F9_08_REV_TOT_TOT,
    F9_09_EXP_TOT_TOT,
    STATE,
    SUBSECTOR
  )

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
      .default = "No Expenses Provided"
    )
  )
    
govt_grant_bysubsector <- efile_merge |>
  dplyr::rename(
    total_expenses = F9_09_EXP_TOT_TOT,
    total_revenue = F9_08_REV_TOT_TOT,
    govt_grant = F9_08_REV_CONTR_GOVT_GRANT
  ) |>
  dplyr::mutate(
    total_expenses = as.numeric(total_expenses),
    total_revenue = as.numeric(total_revenue),
    govt_grant = as.numeric(govt_grant)
  ) |>
  dplyr::group_by(STATE, SUBSECTOR) |>
  dplyr::summarise(
    median_govt_grant = median(govt_grant, na.rm = TRUE),
    proportion_govt_grant = round(
      sum(govt_grant, na.rm = TRUE) / sum(total_revenue, na.rm = TRUE) * 100,
      2
    ),
    number_nonprofits = dplyr::n_distinct(EIN2)
  )

govt_grant_byexpense <- efile_merge |>
  dplyr::rename(
    total_expenses = F9_09_EXP_TOT_TOT,
    total_revenue = F9_08_REV_TOT_TOT,
    govt_grant = F9_08_REV_CONTR_GOVT_GRANT
  ) |>
  dplyr::mutate(
    total_expenses = as.numeric(total_expenses),
    total_revenue = as.numeric(total_revenue),
    govt_grant = as.numeric(govt_grant)
  ) |>
  dplyr::group_by(STATE, expense_category) |>
  dplyr::summarise(
    median_govt_grant = median(govt_grant, na.rm = TRUE),
    proportion_govt_grant = round(
      sum(govt_grant, na.rm = TRUE) / sum(total_revenue, na.rm = TRUE) * 100,
      2
    ),
    number_nonprofits = dplyr::n_distinct(EIN2)
  )

## TODO

# Figure out 990EZ Plan
# government grants need to be extracted from efile data - try xml shennanigans
# common functions
# update BMF with new data
# Only include the 50 states
# put into template
# clean up codebase


