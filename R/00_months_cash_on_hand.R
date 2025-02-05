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
source("R/proportion_govt_grant.R")

# (1) - Download raw data

# Form 990 SOI for 2023, 2022 and 2021 Calendar Year
download.file("https://gt990datalake-rawdata.s3.us-east-1.amazonaws.com/EfileData/Extracts/Data/23eoextract990.xlsx", 
              "data/raw/soi23_raw.xlsx")
download.file("https://gt990datalake-rawdata.s3.us-east-1.amazonaws.com/EfileData/Extracts/Data/22eoextract990.xlsx", 
              "data/raw/soi22_raw.xlsx")
download.file("https://gt990datalake-rawdata.s3.us-east-1.amazonaws.com/EfileData/Extracts/Data/21eoextract990.xlsx", 
              "data/raw/soi21_raw.xlsx")

# Part 08 and 09 Efile data for 2021 tax year
download.file(
  "https://nccs-efile.s3.us-east-1.amazonaws.com/parsed/F9-P08-T00-REVENUE-2021.csv",
  "data/raw/efile_p08_2021.csv"
)
download.file(
  "https://nccs-efile.s3.us-east-1.amazonaws.com/parsed/F9-P09-T00-EXPENSES-2021.csv",
  "data/raw/efile_p09_2021.csv"
)

# Unified BMF
download.file("https://nccsdata.s3.amazonaws.com/harmonized/bmf/unified/BMF_UNIFIED_V1.1.csv",
              "data/raw/unified_bmf.csv")

# 119th Congressional district to 2020 Tract relationship
# https://www2.census.gov/geo/docs/maps-data/data/rel2020/cd-sld/tab20_cd11920_tract20_natl.txt
download.file("https://www2.census.gov/geo/docs/maps-data/data/rel2020/cd-sld/tab20_cd11920_tract20_natl.txt",
              "data/raw/congress_fips.txt")

# 2010 to 2020 Tract relationship
download.file("https://www2.census.gov/geo/docs/maps-data/data/rel2020/tract/tab20_tract20_tract10_natl.txt",
              "data/raw/tract_rship.txt")

# (2) -  Read in and filter datasets

# (2.1) - SOI Data

soi_cols <- list(
  character = c("ein",
                "EIN",
                "rptlndbldgeqptcd"),
  # land, buildings and equipment reported?
  numeric = c(
    "tax_pd",
    "unrstrctnetasstsend",
    # unrestricted net assets
    "lndbldgsequipend",
    # land, buildings, and equipment
    "txexmptbndsend",
    # tax-exempt bond liabilities
    "secrdmrtgsend",
    # secured mortgages and notes payable
    "unsecurednotesend",
    # unsecured mortgages and notes payable
    "totfuncexpns",
    # total functional expenses - same as total expenses
    "deprcatndepletn"
  ) # depreciation
)

# Read in SOI Files and also save them to .csv for easier reading
soi_23 <- readxl::read_xlsx("data/raw/soi23_raw.xlsx")
rio::export(soi_23, "data/raw/soi23_raw.csv")
soi_22 <- readxl::read_xlsx("data/raw/soi22_raw.xlsx")
rio::export(soi_22, "data/raw/soi22_raw.csv")
soi_21 <- readxl::read_xlsx("data/raw/soi21_raw.xlsx")
rio::export(soi_21, "data/raw/soi21_raw.csv")

# Select relevant columns
soi_23 <- data.table::fread("data/raw/soi23_raw.csv", select = soi_cols)
soi_22 <- data.table::fread("data/raw/soi22_raw.csv", select = soi_cols)
soi_21 <- data.table::fread("data/raw/soi21_raw.csv", select = soi_cols)

bmf_cols <- list(
  character = c(
    "EIN2",
    "NTEEV2",
    "NTEE_IRS",
    "CENSUS_STATE_ABBR",
    "ORG_YEAR_LAST",
    "CENSUS_BLOCK_FIPS"
  )
)

# (2.2) - Unified BMF

# Unified BMF with relevant columns selected
unified_bmf <- data.table::fread("data/raw/unified_bmf.csv", 
                                 select = bmf_cols)

# (2.3) - Congressional District Data

congress_district_cols <- list(
  character = c("GEOID_TRACT_20", "NAMELSAD_CD119_20")
)

congress_districts_119 <- data.table::fread("data/raw/congress_fips.txt", 
                                            sep = "|",
                                            select = congress_district_cols)

# (2.4) 2010 to 2020 Tract relationship

tract_rship_cols <- list(
  character = c("GEOID_TRACT_10", "GEOID_TRACT_20")
)
tract_rship <- data.table::fread("data/raw/tract_rship.txt",
                                 sep = "|",
                                 select = tract_rship_cols)

# (2.5) Efile data

efile_cols <- list(
  character = c("ORG_EIN", "TAX_YEAR"),
  numeric = c("F9_08_REV_CONTR_GOVT_GRANT", "F9_08_REV_TOT_TOT")
)

efile_21 <- data.table::fread("data/raw/efile_p08_2021.csv", 
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
                  totfuncexpns < 50000 ~ "Less than $50K",
                  totfuncexpns >= 50000 & totfuncexpns < 100000 ~ "Between $50K and $100K",
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
  dplyr::filter(
    as.integer(ORG_YEAR_LAST) >= 2021
  ) |>
  dplyr::mutate(
    SUBSECTOR = substr(NTEEV2, 1, 3),
    GEOID_TRACT_10 = substr(CENSUS_BLOCK_FIPS, 1, 11),
    CENSUS_REGION = case_when(
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
    ) # Add census regions based on states
  ) |>
  tidylog::left_join(tract_rship, by = "GEOID_TRACT_10") |> # map 2020 tract fips
  tidylog::left_join(congress_districts_119, by = "GEOID_TRACT_20") # Congressional district data

# (3.3) efile data

efile_21 <- efile_21 |>
  dplyr::filter(TAX_YEAR == "2021") |>
  dplyr::mutate(
    EIN2 = purrr::pmap_chr(
      list(ORG_EIN),
      derive_ein2,
      .progress = TRUE
    )
  ) |>
  dplyr::select(F9_08_REV_CONTR_GOVT_GRANT, 
                F9_08_REV_TOT_TOT,
                EIN2)


# (4) Compute metrics 

# (4.1) - Months of Cash on Hand

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

# (4.2) - Proportion of revenue derived from government grants

efile_sample <- efile_21 |>
  dplyr::mutate(
    proportion_govt_grant = purrr::pmap_dbl(
      list(
        F9_08_REV_CONTR_GOVT_GRANT,
        F9_08_REV_TOT_TOT
      ),
      proportion_govt_grant,
      .progress = TRUE
    )
  )

# (5) - Merge datasets and save intermediate data

full_sample_int <- bmf_sample |>
  tidylog::left_join(
    soi_sample,
    by = c("EIN2" = "EIN2")
  ) |>
  tidylog::left_join(
    efile_sample,
    by = c("EIN2" = "EIN2")
  )

data.table::fwrite(full_sample_int, "data/intermediate/full_sample.csv")

# (6) Post process and save intermediate datasets

full_sample_proc <- full_sample_int |>
  dplyr::select(EIN2,
                CENSUS_REGION,
                CENSUS_STATE_ABBR,
                NAMELSAD_CD119_20,
                SUBSECTOR,
                expense_category,
                rptlndbldgeqptcd,
                months_cash_on_hand,
                F9_08_REV_CONTR_GOVT_GRANT,
                proportion_govt_grant
                ) |>
  dplyr::rename(
    CONGRESS_DISTRICT_NAME = NAMELSAD_CD119_20,
    TANGIBLE_ASSETS_REPORTED = rptlndbldgeqptcd,
    GOVERNMENT_GRANT_DOLLAR_AMOUNT = F9_08_REV_CONTR_GOVT_GRANT,
    PROPORTION_GOVT_GRANT = proportion_govt_grant,
    EXPENSE_CATEGORY = expense_category,
    MONTHS_CASH_ON_HAND = months_cash_on_hand 
  )

data.table::fwrite(full_sample_proc, "data/intermediate/full_sample_processed.csv") # Save intermediate

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



# bmf Block FIPs are from 2010 Census, district data is from 2010 census
# relationship file maps 2010 tract to 2020 tract
# 2020 tract - first 11 digits of 2020 block
# Use this to map congressional districts at the tract level

length(intersect(as.character(unified_bmf$CENSUS_BLOCK_FIPS), as.character(xwalk$block.census.geoid)))
