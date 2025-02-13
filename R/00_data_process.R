# Script Header
# Title: Federal Funding Freeze Blog Post
# Date created: 2025-01-31
# Date last modified: 2025-02-12
# Description: This script contains code to download, wrangle, and process data
# for HTML fact sheets on nonprofits's fiscal sustainability and reliance on 
# government grants for Tax Year 2021.

# Create necessary folders
dir.create("data")
dir.create("data/raw")
dir.create("data/intermediate")
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
library(sf)
library(tigris)
states <- as.character(usdata::state_stats$abbr) # 51 states

# Helper Scripts
source("R/spending_on_hand.R") # Formula to calculate months/days cash on hand
source("R/derive_ein2.R") # Function to derive EIN2
source("R/cash_on_hand.R") # Function to calculate cash on hand
source("R/profit_margin.R") # Function to calculate profit margin

# (1) - Download raw data

# (1.1) - Form 990 SOI for 2023, 2022 and 2021 Calendar Year
download.file("https://gt990datalake-rawdata.s3.us-east-1.amazonaws.com/EfileData/Extracts/Data/23eoextract990.xlsx", 
              "data/raw/soi23_raw.xlsx")
download.file("https://gt990datalake-rawdata.s3.us-east-1.amazonaws.com/EfileData/Extracts/Data/22eoextract990.xlsx", 
              "data/raw/soi22_raw.xlsx")
download.file("https://gt990datalake-rawdata.s3.us-east-1.amazonaws.com/EfileData/Extracts/Data/21eoextract990.xlsx", 
              "data/raw/soi21_raw.xlsx")

# (1.2) - Part 08 and 09 Efile data for 2021 tax year

## New datasets created on 11 feb 2025

download.file(
  "https://nccs-efile.s3.us-east-1.amazonaws.com/public/v2025/F9-P08-T00-REVENUE-2021.csv",
  "data/raw/efile_p08_2021_0225.csv"
)
download.file(
  "https://nccs-efile.s3.us-east-1.amazonaws.com/public/v2025/F9-P09-T00-EXPENSES-2021.csv",
  "data/raw/efile_p09_2021_0225.csv"
)
download.file(
  "https://nccs-efile.s3.us-east-1.amazonaws.com/public/v2025/F9-P10-T00-BALANCE-SHEET-2021.csv",
  "data/raw/efile_p10_2021_0225.csv"
)

# (1.3) - BMF Data
download.file("https://nccsdata.s3.amazonaws.com/harmonized/bmf/unified/BMF_UNIFIED_V1.1.csv",
              "data/raw/unified_bmf.csv")

# (2) -  Load Data

# (2.1) - BMF Data

bmf_cols <- list(
  character = c(
    "EIN2",
    "NTEEV2",
    "CENSUS_STATE_ABBR",
    "CENSUS_COUNTY_NAME",
    "ORG_YEAR_LAST",
    "CENSUS_BLOCK_FIPS",
    "BMF_SUBSECTION_CODE",
    "NCCS_LEVEL_1",
    "LATITUDE",
    "LONGITUDE"
  )
)

# (2.2) - Unified BMF

# Unified BMF with relevant columns selected
unified_bmf <- data.table::fread("data/raw/unified_bmf.csv", 
                                 select = bmf_cols)

# (2.3) - Congressional District Data

# From Tigris
cd_tigris <- tigris::congressional_districts()
cd_transformed <- sf::st_transform(cd_tigris, 4326)

# (2.5) Efile data

efile_cols <- list(
  character = c("EIN2", "TAX_YEAR"),
  numeric = c(
    "F9_08_REV_CONTR_GOVT_GRANT",
    "F9_08_REV_TOT_TOT",
    "F9_09_EXP_TOT_TOT",
    "F9_09_EXP_DEPREC_PROG",
    "F9_10_ASSET_CASH_EOY",
    "F9_10_ASSET_SAVING_EOY",
    "F9_10_ASSET_PLEDGE_NET_EOY",
    "F9_10_ASSET_ACC_NET_EOY"
  )
)

efile_21_p08_raw <- data.table::fread("data/raw/efile_p08_2021_0225.csv", 
                              select = efile_cols)
efile_21_p09_raw <- data.table::fread("data/raw/efile_p09_2021_0225.csv",
                                      select = efile_cols)
efile_21_p10_raw <- data.table::fread("data/raw/efile_p10_2021_0225.csv",
                                      select = efile_cols)
# (3) - Wrangle Data

# (3.1) - Wrangle SOI data

data.table::setnames(soi_23, "ein", "EIN")
soi_raw <- data.table::rbindlist(list(soi_23, soi_22, soi_21))

soi_sample <- soi_raw |>
  dplyr::mutate(
    tax_year = substr(tax_pd, 1, 4)
  ) |>
  dplyr::filter(
    tax_year == "2021"
  ) |>
  dplyr::mutate(across(dplyr::all_of(soi_cols$numeric), ~ tidyr::replace_na(., 0)),
                EIN2 = purrr::pmap_chr(
                  list(EIN),
                  derive_ein2,
                  .progress = TRUE
                ),
                expense_category = dplyr::case_when(
                  totfuncexpns < 100000 ~ "Less than $100K",
                  totfuncexpns >= 100000 &
                    totfuncexpns < 500000 ~ "Between $100K and $499K",
                  totfuncexpns >= 500000 &
                    totfuncexpns < 1000000 ~ "Between $500K and $999K",
                  totfuncexpns >= 1000000 &
                    totfuncexpns < 5000000 ~ "Between $1M and $4.99M",
                  totfuncexpns >= 5000000 &
                    totfuncexpns < 10000000 ~ "Between $5M and $9.99M",
                  totfuncexpns >= 10000000 ~ "Greater than $10M",
                  .default = "No Expenses Provided"
                )) # set factor levels

# 318,832 Form 990 tax records

# (3.2) - Wrangle BMF Data

bmf_sample <- unified_bmf |>
  dplyr::filter(NCCS_LEVEL_1 == "501C3 CHARITY") |>
  dplyr::mutate(
    SUBSECTOR = substr(NTEEV2, 1, 3),
    GEOID_TRACT_10 = substr(CENSUS_BLOCK_FIPS, 1, 11),
    CENSUS_REGION = dplyr::case_when(
      CENSUS_STATE_ABBR %in% c("CT", "ME", "MA", "NH", "RI", "VT") ~ "New England",
      CENSUS_STATE_ABBR %in% c("NJ", "NY", "PA") ~ "Mid-Atlantic",
      CENSUS_STATE_ABBR %in% c("IL", "IN", "MI", "OH", "WI") ~ "East North Central",
      CENSUS_STATE_ABBR %in% c("IA", "KS", "MN", "MO", "NE", "ND", "SD") ~ "West North Central",
      CENSUS_STATE_ABBR %in% c("DE", "FL", "GA", "MD", "NC", "SC", "VA", "WV", "DC") ~ "South Atlantic",
      CENSUS_STATE_ABBR %in% c("AL", "KY", "MS", "TN") ~ "East South Central",
      CENSUS_STATE_ABBR %in% c("AR", "LA", "OK", "TX") ~ "West South Central",
      CENSUS_STATE_ABBR %in% c("AZ", "CO", "ID", "MT", "NV", "NM", "UT", "WY") ~ "Mountain",
      CENSUS_STATE_ABBR %in% c("AK", "CA", "HI", "OR", "WA") ~ "Pacific",
      .default = NA
    ),
    EIN2 = format_ein(EIN2, to = "n")
  ) |>
  dplyr::mutate(
    SUBSECTOR = ifelse(SUBSECTOR == "", "UNU", SUBSECTOR),
    EIN2 = format_ein(EIN2, to = "id")
  )

# Merge Congressional districts

bmf_sample <- bmf_sample |>
  dplyr::filter(!is.na(LATITUDE) & !is.na(LONGITUDE)) |>
  sf::st_as_sf(coords = c("LONGITUDE", "LATITUDE"), crs = 4326)

bmf_sample <- sf::st_join(bmf_sample, cd_transformed, join = sf::st_intersects)

# Save intermediate dataset

data.table::fwrite(bmf_sample, "data/intermediate/bmf_sample.csv")

# Select relevant columns

bmf_sample <- bmf_sample |>
  dplyr::select(
    "EIN2",
    "CENSUS_REGION",
    "CENSUS_STATE_ABBR",
    "CENSUS_COUNTY_NAME",
    "ORG_YEAR_LAST",
    "SUBSECTOR",
    "NAMELSAD20"
  )

# (3.3) efile data

# Merge all 3 e-file datasets. Left join to Part VIII since that contains the
# government grant information

efile_sample <- efile_21_p08_raw |>
  tidylog::left_join(
    efile_21_p09_raw,
    by = c("EIN2" = "EIN2")
  ) |>
  tidylog::left_join(
    efile_21_p10_raw,
    by = c("EIN2" = "EIN2")
  )

efile_sample <- efile_sample |>
  dplyr::filter(
    TAX_YEAR == 2021
  ) |>
  dplyr::select(
    EIN2,
    F9_08_REV_CONTR_GOVT_GRANT,
    F9_08_REV_TOT_TOT,
    F9_09_EXP_TOT_TOT,
    F9_09_EXP_DEPREC_PROG,
    F9_10_ASSET_CASH_EOY,
    F9_10_ASSET_SAVING_EOY,
    F9_10_ASSET_PLEDGE_NET_EOY,
    F9_10_ASSET_ACC_NET_EOY
  )

# (4) Compute metrics 

# (4.1) - Days and Months of Cash on Hand

efile_sample <- efile_sample |>
  dplyr::mutate(
    months_cash_on_hand = purrr::pmap_dbl(
      list(
        F9_10_ASSET_CASH_EOY,
        F9_10_ASSET_SAVING_EOY,
        F9_10_ASSET_PLEDGE_NET_EOY,
        F9_10_ASSET_ACC_NET_EOY,
        F9_09_EXP_TOT_TOT,
        F9_09_EXP_DEPREC_PROG
      ),
      calculate_cash_duration,
      unit = "months",
      .progress = TRUE
    ),
    days_cash_on_hand = purrr::pmap_dbl(
      list(
        F9_10_ASSET_CASH_EOY,
        F9_10_ASSET_SAVING_EOY,
        F9_10_ASSET_PLEDGE_NET_EOY,
        F9_10_ASSET_ACC_NET_EOY,
        F9_09_EXP_TOT_TOT,
        F9_09_EXP_DEPREC_PROG
      ),
      calculate_cash_duration,
      unit = "days",
      .progress = TRUE
    )
  )

# (4.2) - Profit Margin - with and without government grants

efile_sample <- efile_sample |>
  dplyr::mutate(
    profit_margin = purrr::pmap_dbl(
      list(
        F9_08_REV_TOT_TOT,
        F9_09_EXP_TOT_TOT
      ),
      profit_margin,
      .progress = TRUE
    )
  ) |>
  dplyr::mutate(
    profit_margin_nogovtgrant = purrr::pmap_dbl(
      list(
        F9_08_REV_TOT_TOT,
        F9_09_EXP_TOT_TOT,
        F9_08_REV_CONTR_GOVT_GRANT
      ),
      profit_margin,
      .progress = TRUE
    )
  )

# (5) - Merge datasets and save intermediate data

full_sample_int <- efile_sample |>
  tidylog::left_join(
    bmf_sample,
    by = c("EIN2" = "EIN2")
  )

data.table::fwrite(full_sample_int, "data/intermediate/full_sample.csv")

# (6) Post process and save intermediate datasets

full_sample_proc <- full_sample_int |>
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
    ),
    SUBSECTOR = dplyr::case_when(
      SUBSECTOR == "ART" ~ "Arts, Culture, and Humanities",
      SUBSECTOR == "EDU" ~ "Education (Excluding Universities)",
      SUBSECTOR == "ENV" ~ "Environment and Animals",
      SUBSECTOR == "HEL" ~ "Health (Excluding Hospitals)",
      SUBSECTOR == "HMS" ~ "Human Services",
      SUBSECTOR == "IFA" ~ "International, Foreign Affairs",
      SUBSECTOR == "PSB" ~ "Public, Societal Benefit",
      SUBSECTOR == "REL" ~ "Religion Related",
      SUBSECTOR == "MMB" ~ "Mutual/Membership Benefit",
      SUBSECTOR == "UNU" ~ "Unclassified",
      SUBSECTOR == "UNI" ~ "Universities",
      SUBSECTOR == "HOS" ~ "Hospitals",
      TRUE ~ "Unclassified"  # Default case for unmatched codes
    )
  ) |>
  dplyr::select(EIN2,
                CENSUS_REGION,
                CENSUS_COUNTY_NAME,
                CENSUS_STATE_ABBR,
                NAMELSAD20,
                SUBSECTOR,
                expense_category,
                F9_08_REV_CONTR_GOVT_GRANT,
                profit_margin,
                profit_margin_nogovtgrant,
                months_cash_on_hand,
                days_cash_on_hand
                ) |>
  dplyr::rename(
    CONGRESS_DISTRICT_NAME = NAMELSAD20,
    GOVERNMENT_GRANT_DOLLAR_AMOUNT = F9_08_REV_CONTR_GOVT_GRANT,
    EXPENSE_CATEGORY = expense_category,
    PROFIT_MARGIN = profit_margin,
    PROFIT_MARGIN_NOGOVTGRANT = profit_margin_nogovtgrant,
    MONTHS_CASH_ON_HAND = months_cash_on_hand,
    DAYS_CASH_ON_HAND = days_cash_on_hand
  )

data.table::fwrite(full_sample_proc, "data/intermediate/full_sample_processed.csv")

## TODO

# Update EIN2 for the Unified BMF

# Questions for Jesse

# For the total number of nonprofits, should I use the 2025 BMF?
# How should I be thinking about duplicate EINs in the efile data or after merging?
# Is my spatoal join with the congressional districts correct?