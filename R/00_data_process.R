# Script Header
# Title: Federal Funding Freeze Blog Post
# Date created: 2025-01-31
# Date last modified: 2025-02-17
# Description: This script contains code to download, wrangle, and process data
# for HTML fact sheets on nonprofits's fiscal sustainability and reliance on 
# government grants for Tax Year 2021.

# Create necessary folders to store data
dir.create("data")
dir.create("data/raw")
dir.create("data/intermediate")
dir.create("data/processed")

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
states <- as.character(usdata::state_stats$abbr) # Names of 50 states + DC

# Helper Scripts
source("R/format_ein.R") # Function to format ein to EIN 2
source("R/cash_on_hand.R") # Functions to calculate cash on hand by either day or month
source("R/profit_margin.R") # Function to calculate profit margin
source("R/operating_reserve_ratio.R") # Function to calculate operating reserve ratio

# (1) - Download raw data

# (1.1) - Form 990 SOI for 2023, 2022 and 2021 Calendar Year
download.file("https://gt990datalake-rawdata.s3.us-east-1.amazonaws.com/EfileData/Extracts/Data/23eoextract990.xlsx", 
              "data/raw/soi23_raw.xlsx")
download.file("https://gt990datalake-rawdata.s3.us-east-1.amazonaws.com/EfileData/Extracts/Data/22eoextract990.xlsx", 
              "data/raw/soi22_raw.xlsx")
download.file("https://gt990datalake-rawdata.s3.us-east-1.amazonaws.com/EfileData/Extracts/Data/21eoextract990.xlsx", 
              "data/raw/soi21_raw.xlsx")

# (1.2) - Part 01, 08, 09 and 10 Efile data for tax year 2021

## New datasets created on 11 feb 2025
download.file(
  "https://nccs-efile.s3.us-east-1.amazonaws.com/public/v2025/F9-P01-T00-SUMMARY-2021.csv",
  "data/raw/efile_p01_2021_0225.csv"
)

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

# (1.3) - Unified BMF Data
download.file("https://nccsdata.s3.amazonaws.com/harmonized/bmf/unified/BMF_UNIFIED_V1.1.csv",
              "data/raw/unified_bmf.csv")

# (2) -  Load in Data with the necessary columns and datatypes

# (2.1) - BMF Data

bmf_cols <- list(
  character = c(
    "EIN2",
    "NTEEV2",
    "CENSUS_STATE_ABBR",
    "CENSUS_COUNTY_NAME",
    "ORG_YEAR_FIRST",
    "ORG_YEAR_LAST",
    "CENSUS_BLOCK_FIPS",
    "BMF_SUBSECTION_CODE",
    "NCCS_LEVEL_1"
  ),
  numeric = c("LATITUDE", "LONGITUDE")
)

unified_bmf <- data.table::fread("data/raw/unified_bmf.csv", 
                                 select = bmf_cols)

# (2.2) - Congressional District Data

# From Tigris
cd_tigris <- tigris::congressional_districts()
cd_transformed <- sf::st_transform(cd_tigris, 4326)

# (2.3) Efile data

efile_cols <- list(
  character = c("EIN2", "TAX_YEAR", "RETURN_TYPE", "OBJECTID", "URL"),
  numeric = c(
    "F9_08_REV_CONTR_GOVT_GRANT", # Total government grants - Part 8
    "F9_08_REV_TOT_TOT", # Total revenue - Part 8
    "F9_09_EXP_TOT_TOT", # Total expenses - Part 8
    "F9_09_EXP_DEPREC_PROG", # Depreciation - Part 9
    "F9_10_ASSET_CASH_EOY", # Total Cash - Part 10
    "F9_10_ASSET_SAVING_EOY",
    "F9_10_ASSET_PLEDGE_NET_EOY",
    "F9_10_ASSET_ACC_NET_EOY",
    "F9_10_NAFB_UNRESTRICT_EOY",
    "F9_10_ASSET_LAND_BLDG_NET_EOY", 
    "F9_10_LIAB_TAX_EXEMPT_BOND_EOY", 
    "F9_10_LIAB_MTG_NOTE_EOY",
    "F9_10_LIAB_NOTE_UNSEC_EOY",
    "F9_01_EXP_TOT_CY", # Total Expenses - Part 1
    "F9_01_REV_TOT_CY", # Total revenue - Part 1
    "F9_09_EXP_DEPREC_TOT",
    "F9_01_NAFB_TOT_EOY"
  )
)

efile_21_p08_raw <- data.table::fread("data/raw/efile_p08_2021_0225.csv", select = efile_cols)
efile_21_p09_raw <- data.table::fread("data/raw/efile_p09_2021_0225.csv", select = efile_cols)
efile_21_p10_raw <- data.table::fread("data/raw/efile_p10_2021_0225.csv", select = efile_cols)
efile_21_p01_raw <- data.table::fread("data/raw/efile_p01_2021_0225.csv", select = efile_cols)

## Get counts and the relevant summary stats for testing.

### Number of 990 e-file records for 2021 from part 08
numrec_w_part08 <- efile_21_p08_raw |>
  dplyr::filter(
    TAX_YEAR == "2021",
    RETURN_TYPE == "990"
  ) |>
  nrow()

### Number of 990 e-file records for 2021 reporting government grants
numrec_w_gvgrnt <- efile_21_p08_raw |>
  dplyr::filter(
    !is.na(F9_08_REV_CONTR_GOVT_GRANT) &
      F9_08_REV_CONTR_GOVT_GRANT != 0,
    TAX_YEAR == "2021",
    RETURN_TYPE == "990"
  ) |>
  nrow()

### Total government grants reported in 2021
total_gvgrnt <- efile_21_p08_raw |>
  dplyr::filter(
    !is.na(F9_08_REV_CONTR_GOVT_GRANT) &
      F9_08_REV_CONTR_GOVT_GRANT != 0,
    TAX_YEAR == "2021",
    RETURN_TYPE == "990"
  ) |>
  dplyr::summarise(
    total_gvgrnt = sum(F9_08_REV_CONTR_GOVT_GRANT)
  ) |>
  dplyr::pull(total_gvgrnt) # $298,466,270,662

# (2.4) - SOI Data (currently unused, do not run this step if you do not need it)

soi_cols <- list(
  character = c("EIN", "ein", "tax_pd"),
  numeric = c("totfuncexpns")
)

soi_23 <- data.table::fread("data/raw/soi23_raw.csv", 
                            select = soi_cols)
soi_22 <- data.table::fread("data/raw/soi22_raw.csv",
                            select = soi_cols)
soi_21 <- data.table::fread("data/raw/soi21_raw.csv",
                            select = soi_cols)

soi_23 <- soi_23 |>
  dplyr::rename("EIN" = ein)

soi_sample <- data.table::rbindlist(list(soi_23, soi_22, soi_21))

# (3) - Wrangle Data

# (3.1) - Wrangle BMF Data

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
      .default = "Unmapped"
    ),
    EIN2 = format_ein(EIN2, to = "n")
  ) |>
  dplyr::mutate(
    SUBSECTOR = ifelse(SUBSECTOR == "", "UNU", SUBSECTOR),
    EIN2 = format_ein(EIN2, to = "id")
  )

## QC Check - No extra rows were dropped outside of filter statement

nrow(bmf_sample) == nrow(unified_bmf[unified_bmf$NCCS_LEVEL_1 == "501C3 CHARITY"])

# Merge Congressional districts

bmf_sample <- bmf_sample |>
  sf::st_as_sf(coords = c("LONGITUDE", "LATITUDE"), crs = 4326)

bmf_sample <- sf::st_join(bmf_sample, cd_transformed, join = sf::st_intersects)

# Save intermediate dataset after spatial join. This is the BMF sample

data.table::fwrite(bmf_sample, "data/intermediate/bmf_sample.csv")
rm(unified_bmf, cd_tigris, cd_transformed)
gc()

# (3.2) Wrangle efile data

# Merge all 3 e-file datasets. Left join to Part VIII since that contains the
# government grant information

efile_sample <- efile_21_p08_raw |>
  dplyr::filter(!is.na(F9_08_REV_CONTR_GOVT_GRANT),
                F9_08_REV_CONTR_GOVT_GRANT != 0) |>
  tidylog::left_join(efile_21_p09_raw) |>
  tidylog::left_join(efile_21_p10_raw) |>
  tidylog::left_join(efile_21_p01_raw)

nrow(efile_sample) == numrec_w_gvgrnt #11 records have been added
sum(efile_sample$F9_08_REV_CONTR_GOVT_GRANT) == total_gvgrnt  # $298,469,954,367, $3 million added

## Isolate duplicates

duplicates <- efile_sample |>
  dplyr::group_by(EIN2) |>
  dplyr::filter(dplyr::n() > 1)

## Optional: To save memory
rm(efile_21_p08_raw, efile_21_p09_raw, efile_21_p10_raw, efile_21_p01_raw)
gc()

## Wrangle
efile_sample <- efile_sample |>
  dplyr::filter(
    TAX_YEAR == 2021,
    RETURN_TYPE == "990"
  ) |>
  dplyr::select(
    "EIN2",
    "F9_08_REV_CONTR_GOVT_GRANT",
    "F9_08_REV_TOT_TOT",
    "F9_09_EXP_TOT_TOT",
    "F9_09_EXP_DEPREC_PROG",
    "F9_09_EXP_DEPREC_TOT",
    "F9_10_ASSET_CASH_EOY",
    "F9_10_ASSET_SAVING_EOY",
    "F9_10_ASSET_PLEDGE_NET_EOY",
    "F9_10_ASSET_ACC_NET_EOY",
    "F9_10_NAFB_UNRESTRICT_EOY",
    "F9_10_ASSET_LAND_BLDG_NET_EOY",
    "F9_10_LIAB_TAX_EXEMPT_BOND_EOY",
    "F9_10_LIAB_MTG_NOTE_EOY",
    "F9_10_LIAB_NOTE_UNSEC_EOY",
    "F9_01_EXP_TOT_CY",
    "F9_01_REV_TOT_CY",
    "F9_01_NAFB_TOT_EOY"
  )

nrow(efile_sample) == numrec_w_gvgrnt #TRUE
sum(efile_sample$F9_08_REV_CONTR_GOVT_GRANT) == total_gvgrnt  # TRUE

# (3.3) - Wrangle SOI Data

soi_sample <- soi_sample |>
  dplyr::mutate(
    tax_year = substr(tax_pd, 1, 4)
  ) |>
  dplyr::filter(
    tax_year == "2021"
  ) |>
  dplyr::mutate(EIN2 = format_ein(EIN, to = "n"),
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
                )) |>
  dplyr::mutate(
    EIN2 = format_ein(EIN2, to = "id")
  ) |>
  dplyr::select(EIN2, expense_category) |>
  dplyr::rename(EXPENSE_CATEGORY = expense_category)

# Merge with BMF - If you need SOI columns

bmf_sample <- bmf_sample |>
  tidylog::left_join(soi_sample)

# (4) Compute metrics 

# (4.1) - Profit Margin - with and without government grants

# The tails are fat so we will get extreme values
create_sorted_plot(efile_sample, "F9_01_REV_TOT_CY")
create_sorted_plot(efile_sample, "F9_01_EXP_TOT_CY")

efile_sample <- efile_sample |>
  dplyr::mutate(profit_margin = purrr::pmap_dbl(
    list(F9_01_REV_TOT_CY, F9_01_EXP_TOT_CY),
    profit_margin,
    .progress = TRUE
  ))

summary(efile_sample$profit_margin)

create_sorted_plot(efile_sample, "profit_margin")

# purrr::pmap_dbl does not work for some reason. I will use a rowwise mutate instead
efile_sample <- efile_sample |>
  dplyr::rowwise() |>
  dplyr::mutate(
    profit_margin_nogovtgrant = {
      if (dplyr::cur_group_rows() %% 10000 == 0) cat(".")  # prints a dot every 10000 rows
      profit_margin(
        F9_01_REV_TOT_CY,
        F9_01_EXP_TOT_CY,
        F9_08_REV_CONTR_GOVT_GRANT
      )
    }
  ) |>
  dplyr::ungroup()

summary(efile_sample$profit_margin_nogovtgrant)

create_sorted_plot(efile_sample, "profit_margin_nogovtgrant")

# (4.4) At Risk Indicator Variable - if profit margin negative

efile_sample <- efile_sample |>
  dplyr::mutate(
    at_risk = ifelse(profit_margin_nogovtgrant < 0, 1, 0)
  )

summary(efile_sample$at_risk)
table(efile_sample$at_risk)
# 39,514 not at risk, 77,717 at risk

# (5) - Merge datasets and save intermediate data. Merge by the most recent BMF record

full_sample_int <- efile_sample |>
  tidylog::left_join(
    bmf_sample <- bmf_sample |>
      dplyr::arrange(ORG_YEAR_LAST),
    by = c("EIN2" = "EIN2"),
    multiple = "last"
  )

sum(full_sample_int$at_risk) == 77717 # TRUE
sum(full_sample_int$F9_08_REV_CONTR_GOVT_GRANT) == total_gvgrnt # TRUE
nrow(full_sample_int) == numrec_w_gvgrnt # TRUE

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
      .default = "Unclassified"  # Default case for unmatched codes
    )
  ) |>
  dplyr::mutate(
    CENSUS_STATE_NAME = dplyr::case_when(
      CENSUS_STATE_ABBR %in% states ~ usdata::abbr2state(CENSUS_STATE_ABBR),
      CENSUS_STATE_ABBR %in% c("AS", "GU", "MP", "PR", "VI") ~ "Other US Jurisdictions",
      .default = "Unmapped"
    ),
    CONGRESS_DISTRICT_NAME = ifelse(
      is.na(NAMELSAD20, "Unmapped", NAMELSAD20)
    )
  ) |>
  dplyr::select(EIN2,
                CENSUS_REGION,
                CENSUS_COUNTY_NAME,
                CENSUS_STATE_NAME,
                NAMELSAD20,
                SUBSECTOR,
                expense_category,
                F9_08_REV_CONTR_GOVT_GRANT,
                profit_margin,
                profit_margin_nogovtgrant,
                at_risk
                ) |>
  dplyr::rename(
    CONGRESS_DISTRICT_NAME = NAMELSAD20,
    GOVERNMENT_GRANT_DOLLAR_AMOUNT = F9_08_REV_CONTR_GOVT_GRANT,
    EXPENSE_CATEGORY = expense_category,
    PROFIT_MARGIN = profit_margin,
    PROFIT_MARGIN_NOGOVTGRANT = profit_margin_nogovtgrant,
    AT_RISK_NUM = at_risk
  )

##  Check Counts
sum(table(full_sample_proc$CENSUS_REGION)) == numrec_w_gvgrnt
sum(table(full_sample_proc$CENSUS_STATE_NAME)) == numrec_w_gvgrnt
sum(table(full_sample_proc$EXPENSE_CATEGORY)) == numrec_w_gvgrnt
sum(table(full_sample_proc$SUBSECTOR)) == numrec_w_gvgrnt
sum(table(full_sample_proc$CONGRESS_DISTRICT_NAME)) == numrec_w_gvgrnt
sum(full_sample_proc$AT_RISK_NUM) == 77717
sum(full_sample_proc$GOVERNMENT_GRANT_DOLLAR_AMOUNT) == total_gvgrnt

data.table::fwrite(full_sample_proc, "data/intermediate/full_sample_processed.csv")

## TODO

# change days_cash_on_hand to months_cash_on_hand_jesse
# Update EIN2 for the Unified BMF
# Add 2010 fips to BMF for working with crosswalks - both 2010 and 2020 FIPs
    # If a tract is changed between 2 census periods, the tract id will change. I don;t know if the block will change. There is not the same versioning convention with the block. That might suggest the block is not identical. Figure out how tract and block ids change between 2010 and 2020. Important to check sizes of the dataset before/after merges. 

# Questions for Jesse

# How should I be thinking about duplicate EINs in the efile data or after merging?
  # Just merge part 08 and part 09
  # Ammended group and partial returns
  # figure out which is the correct one - first check for amended returns and then use the amended return. If there are multiple, sort by timestamp or filing data. For group returns, if it is a federated organization, I would file one group return for the headquarter org and then file a second return for all of the organizations. The first one they would not check its a group return and the second one they would. The easy is to just drop the group returns. When doing state level analaysis, the group returns are not useful. The headquarters would just be the headquarter in whatever state. Partial returns - use the column for number of days within the tax filing. If you petition the IRS to change your fiscal year, from june to december then what happens is you have to file a partial return to fill the gap. So your fiscal year would end in june and you would file a new return for july to december so that you are falling within compliance fora partial year. If its like a very specific sample, you would have to adjust those numbers individually. Easy way to do it is just to drop those cases. Typically you would have a prior year return for the organization. It would be easier yto use 1 of those 2 years instead of using the partials because it's complicated due to grant cycles etc. Typically we've just thrown those out. If there are duplicates after all this then that's a problem. From the index there were 8 cases with distinct object IDs and identical filings - my best guess is the connection was unstable when submitting and they hit submit again. If I am finding duplicated EINs, it just means something went wrong.



# Integrate nccstools with nccsdata - common functions for data pulls
# efile - merge both packages. keep in R, see if they are doing anything better and integrate it
# if we can speed it up with AWS functions - disaggregate into scripts or turn
# into an internal package etc.

# Notes: The number of filers won't be the total by state because some nonprofits
# are outside the 51 states