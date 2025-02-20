# Script Header
# Title: Federal Funding Freeze Blog Post
# Date created: 2025-01-31
# Date last modified: 2025-02-18
# Description: This script contains code to download, wrangle, and process data
# for HTML fact sheets on nonprofits's fiscal sustainability and reliance on 
# government grants for Tax Year 2021.
### Details:
# (1) - Download raw data
# (2) - Load in and filter data
# (3) - Create the sample dataset from the efile data
# (4) - Wrangle Data
# (5) - Compute fiscal sustainability metrics
# (6) - Merge with the geographic data from the Unified BMF
# (7) - Geographic post processing. Map null geographies with coordinates but without state, county, or district information. Ensure sample contains all possible state and county/district combinations.
# (8) - Post process and save intermediate and processed sample datasets

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
library(lubridate)
library(tidylog)
library(usdata)
library(sf)
library(tigris)
states <- as.character(usdata::state_stats$abbr) # Names of 50 states + DC

# Helper Scripts
source("R/data.R") # Contains URLs to raw data
source("R/download_data.R") # Function to download efile data
source("R/format_ein.R") # Function to format ein to EIN 2
source("R/profit_margin.R") # Function to calculate profit margin
source("R/create_sorted_plot.R") # Function to create sorted plots

# (1) - Download raw data

# (1.1) - Header, Parts 01, 08, 09 and 10 Efile data for tax year 2021. These are new efile datasets created on 11 feb 2025. 

download_files(efile_urls, "data/raw")

# (1.2) - Unified BMF Data

download_files(bmf_urls, "data/raw")

# (1.3) - Non-US Based nonprofits
download_files(xx_urls, "data/raw")

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

# (2.2) - Tigris data. We want to ensure our sample is complete or at the very least aware of the presence of missing states, counties and/or districts.

## State data
state_tigris <- tigris::states()
state_tigris <- state_tigris |>
  dplyr::select("STATEFP", "STUSPS", "NAME") |>
  dplyr::rename(
    "CENSUS_STATE_FIPS" = STATEFP,
    "CENSUS_STATE_ABBR" = STUSPS,
    "CENSUS_STATE_NAME" = NAME
  )

## County Data
county_tigris <- tigris::counties()
county_tigris <- county_tigris |>
  dplyr::select(STATEFP, NAMELSAD, COUNTYFP) |>
  dplyr::rename(
    "CENSUS_STATE_FIPS" = STATEFP,
    "CENSUS_COUNTY_NAME" = NAMELSAD,
    "CENSUS_COUNTY_FIPS" = COUNTYFP
  )

## Congressional Districts (2020)
cd_tigris <- tigris::congressional_districts()
cd_transformed <- sf::st_transform(cd_tigris, 4326)
cd_transformed <- cd_transformed |>
  dplyr::rename("CENSUS_STATE_FIPS" = STATEFP20)

## Map states to counties
county_state <- data.frame(county_tigris) |>
  tidylog::left_join(data.frame(state_tigris), by = "CENSUS_STATE_FIPS")

## Map congressional districts to states
district_state <- data.frame(cd_transformed) |>
  tidylog::left_join(data.frame(state_tigris), by = "CENSUS_STATE_FIPS")

# (2.3) Efile data

efile_cols <- list(
  character = c(
    "EIN2", # EIN formatted to 9 digits like EIN-XX-XXXXXXX
    "TAX_YEAR", # Tax year of the return
    "RETURN_TYPE", # Type of return (990 or 990EZ)
    "OBJECTID", # Unique identifier
    "URL", # URL to the raw return
    "F9_00_EXEMPT_STAT_501C3_X", # Indicates if the organization is a 501c3 public charity (since only public charities file form 990, private foundations file form 990PF)
    "RETURN_TIME_STAMP" # Date and time return was filed
  ),
  numeric = c(
    "F9_08_REV_CONTR_GOVT_GRANT",
    # Total government grants - Part 8
    "F9_08_REV_TOT_TOT",
    # Total revenue - Part 8
    "F9_09_EXP_TOT_TOT",
    # Total expenses - Part 8
    "F9_09_EXP_DEPREC_PROG",
    # Depreciation - Part 9
    "F9_10_ASSET_CASH_EOY",
    # Total Cash - Part 10
    "F9_10_ASSET_SAVING_EOY",
    # Savings and temporary cash investments, end of year - Part 10
    "F9_10_ASSET_PLEDGE_NET_EOY",
    # Pledges and grants receivable, net, end of year - Part 10
    "F9_10_ASSET_ACC_NET_EOY",
    # Net accounts receivable, end of year - Part 10
    "F9_10_NAFB_UNRESTRICT_EOY",
    # Net assets without donor restrictions, end of year - Part 10
    "F9_10_ASSET_LAND_BLDG_NET_EOY",
    # Net value including lands, buildings, and equipment, end of year - Part 10
    "F9_10_LIAB_TAX_EXEMPT_BOND_EOY",
    # Tax exempt bond liabilities, end of year - Part 10
    "F9_10_LIAB_MTG_NOTE_EOY",
    # Secured mortgages and notes payable to unrelated third parties, end of year - Part 10
    "F9_10_LIAB_NOTE_UNSEC_EOY",
    # Unsecured notes and loans payable to unrelated third parties, end of year - Part 10
    "F9_01_EXP_TOT_CY",
    # Total expenses, current year - Part 1
    "F9_01_REV_TOT_CY",
    # Total revenue, current year - Part 1
    "F9_09_EXP_DEPREC_TOT",
    # Depreciation, depletion, and amortization - Part 9
    "F9_01_NAFB_TOT_EOY" # Net assets or fund balances, end of year - Part 1
  ),
  logical = c(
    "RETURN_PARTIAL_X", # Indicates if return is a partial return
    "RETURN_GROUP_X", # Indicates if return is a group return
    "RETURN_AMENDED_X" # Indicates if return is an amended return
  )
)

efile_21_hd_raw <- data.table::fread("data/raw/efile_hd_2021_0225.csv", select = efile_cols)
efile_21_p08_raw <- data.table::fread("data/raw/efile_p08_2021_0225.csv",
                                      select = efile_cols)
efile_21_p09_raw <- data.table::fread("data/raw/efile_p09_2021_0225.csv", select = efile_cols)
efile_21_p10_raw <- data.table::fread("data/raw/efile_p10_2021_0225.csv", select = efile_cols)
efile_21_p01_raw <- data.table::fread("data/raw/efile_p01_2021_0225.csv", select = efile_cols)

# (3) - Create the sample dataset from the efile data

# (3.1) - Exclude foreign nonprofits

eo_xx <- data.table::fread("data/raw/foreign_nonprofits.csv")
eo_xx <- eo_xx |>
  dplyr::mutate(
    EIN2 = format_ein(EIN, to = "n")
  ) |>
  dplyr::mutate(
    EIN2 = format_ein(EIN2, to = "id")
  )
length(intersect(efile_21_p08_raw$EIN2, eo_xx$EIN2)) 

### 537 EINs belong to foreign nonprofits

foreign_ein <- unique(eo_xx$EIN2)

# (3.2) - Only include 501c3 public charities

eins_501c3 <- efile_21_hd_raw |>
  dplyr::filter(F9_00_EXEMPT_STAT_501C3_X == "X") |>
  dplyr::select(EIN2) |>
  dplyr::distinct() |>
  dplyr::pull(EIN2)

length(eins_501c3)

# 392, 704 501c3 public charities

# (3.3) - Filter form 990 records from 501c3 public charities, filed in tax year 2021, and not from the list from foreign EINs

efile_21_p08 <- efile_21_p08_raw |>
  dplyr::filter(
    TAX_YEAR == "2021",
    RETURN_TYPE == "990",
    ! EIN2 %in% foreign_ein,
    EIN2 %in% eins_501c3
  ) |>
  dplyr::mutate(
    RETURN_TIME_STAMP = lubridate::ymd_hms(RETURN_TIME_STAMP)
  )

### 246,018 records

# (3.4) - Process partial, group and amended returns. Retain the most recent returns for group and amended returns. Keep all partial returns.

## Count partial, group and amended returns

num_partial_returns <- efile_21_p08 |>
  dplyr::filter(RETURN_PARTIAL_X == TRUE) |>
  nrow()

### 2,085 partial returns. We leave these as-is. If a nonprofit reports government grants on the partial return it still falls within the tax year and would not be counted otherwise, so it's different than other duplicates that are double counting the same grant.

num_group_returns <- efile_21_p08 |>
  dplyr::filter(RETURN_GROUP_X == TRUE) |>
  nrow()

### 309 group returns

num_amended_returns <- efile_21_p08 |>
  dplyr::filter(RETURN_AMENDED_X == TRUE) |>
  nrow()

### 3977 amended returns

## Retrieve the most recent returns for group returns

group_eins <- efile_21_p08 |>
  dplyr::filter(RETURN_GROUP_X == TRUE) |>
  dplyr::pull(EIN2) |>
  unique()

### 306 unique EINs

efile_grp <- efile_21_p08 |>
  dplyr::filter(EIN2 %in% group_eins)

### 310 records

efile_nogrp <- efile_21_p08 |>
  dplyr::filter(!EIN2 %in% group_eins)

### 245,708 records

## Only retrieve most recent group returns and attach them back to the dataset

efile_grp <- efile_grp |>
  group_by(EIN2) %>%
  slice_max(order_by = RETURN_TIME_STAMP)

### 306 records: 4 duplicates discarded

nrow(efile_grp) == length(group_eins) # TRUE
efile_21_p08 <- bind_rows(efile_nogrp, efile_grp)

### 246,014 records. 4 duplicates discarded.

## Retrieve the most recent returns for amended returns

amended_eins <- efile_21_p08 |>
  dplyr::filter(RETURN_AMENDED_X == TRUE) |>
  dplyr::pull(EIN2) |>
  unique()

### 3,867 Unique EINs

efile_amended <- efile_21_p08 |>
  dplyr::filter(EIN2 %in% amended_eins)

### 7,378 records

efile_noamend <- efile_21_p08 |>
  dplyr::filter(!EIN2 %in% amended_eins)

### 238, 636 records (total still 246,014)

## Only retrieve most recent amended returns and attach them back to the dataset

efile_amended <- efile_amended |>
  group_by(EIN2) %>%
  slice_max(order_by = RETURN_TIME_STAMP)

### 3,867 records: 3,511 duplicates discarded

nrow(efile_amended) == length(amended_eins) # TRUE
efile_21_p08 <- bind_rows(efile_noamend, efile_amended)

### 242,503 records. 3,511 duplicates discarded.

# (3.5) - Get the necessary counts for quality assurance

## Retrieve Number of 990 e-file records for 2021 from part 08 belonging to 501c3 public charities and US nonprofits

numrec_w_part08 <- efile_21_p08 |>
  nrow()

### 242, 503

## Retrieve Number of 990 e-file records for 2021 from part 08 belonging to 501c3 public charities in the US that report government grants

numrec_w_gvgrnt <- efile_21_p08 |>
  dplyr::filter(
    !is.na(F9_08_REV_CONTR_GOVT_GRANT) &
      F9_08_REV_CONTR_GOVT_GRANT != 0
  ) |>
  nrow()

### 103, 475 records

## Calculate the Total government grants reported in 2021  from part 08 belonging to 501c3 public charities in the US

total_gvgrnt <- efile_21_p08 |>
  dplyr::filter(
    !is.na(F9_08_REV_CONTR_GOVT_GRANT) &
      F9_08_REV_CONTR_GOVT_GRANT != 0
  ) |>
  dplyr::summarise(
    total_gvgrnt = sum(F9_08_REV_CONTR_GOVT_GRANT)
  ) |>
  dplyr::pull(total_gvgrnt)

### $267,700,640,005

# (4) - Wrangle Data

# (4.1) - Wrangle BMF Data

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

## Map BMF coordinates to Congressional districts

bmf_sample <- bmf_sample |>
  sf::st_as_sf(coords = c("LONGITUDE", "LATITUDE"), crs = 4326)

bmf_sample <- sf::st_join(bmf_sample, cd_transformed, join = sf::st_intersects)

## Save intermediate dataset after spatial join. This is the BMF sample

data.table::fwrite(bmf_sample, "data/intermediate/bmf_sample.csv")

## Optional: To save memory

rm(unified_bmf, cd_tigris)
gc()

# (4.2) Wrangle efile data

## Merge all 3 e-file datasets. Left join to Part VIII since that contains the government grant information

efile_sample <- efile_21_p08 |>
  dplyr::filter(!is.na(F9_08_REV_CONTR_GOVT_GRANT),
                F9_08_REV_CONTR_GOVT_GRANT != 0) |>
  tidylog::left_join(efile_21_p09_raw) |>
  tidylog::left_join(efile_21_p10_raw) |>
  tidylog::left_join(efile_21_p01_raw)

nrow(efile_sample) == numrec_w_gvgrnt 

### No new records added

sum(efile_sample$F9_08_REV_CONTR_GOVT_GRANT) == total_gvgrnt  

### Total remains the same

## Optional: To save memory

rm(efile_21_p08_raw, 
   efile_21_p09_raw, 
   efile_21_p10_raw, 
   efile_21_p01_raw,
   efile_21_hd_raw,
   efile_21_p08)
gc()

## Wrangle efile data

efile_sample <- efile_sample |>
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

# (5) Compute fiscal sustainability metrics

# (5.1) - Profit Margin - with and without government grants

## Note: sorted plots are used to explore Data - The tails are fat so we will get extreme values

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

## Note:  purrr::pmap_dbl does not work for some reason. I will use a rowwise mutate instead

efile_sample <- efile_sample |>
  dplyr::rowwise() |>
  dplyr::mutate(
    profit_margin_nogovtgrant = {
      if (dplyr::cur_group_rows() %% 10000 == 0) cat(".")  # prints a dot every 10000 rows as a hacky progress bar
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

# (5.2) At Risk Indicator Variable - if profit margin negative

efile_sample <- efile_sample |>
  dplyr::mutate(
    at_risk = ifelse(profit_margin_nogovtgrant < 0, 1, 0)
  )

summary(efile_sample$at_risk)
table(efile_sample$at_risk)

### 33,788 not at risk, 69,687 at risk

# (6) - Merge with the geographic data from the Unified BMF

full_sample_int <- efile_sample |>
  tidylog::left_join(
    bmf_sample <- bmf_sample |>
      dplyr::arrange(ORG_YEAR_LAST),
    by = c("EIN2" = "EIN2"),
    multiple = "last"
  )

sum(full_sample_int$at_risk) == sum(efile_sample$at_risk) # TRUE
sum(full_sample_int$F9_08_REV_CONTR_GOVT_GRANT) == total_gvgrnt # TRUE
nrow(full_sample_int) == numrec_w_gvgrnt # TRUE

# (7) - Geographic post processing. Map null geographies with coordinates but without state, county, or district information. Ensure sample contains all possible state and county/district combinations.

# (7.1) - Missing state information

# Identify records with missing state but valid geometry

null_geoms <- full_sample_int |>
  dplyr::filter(is.na(CENSUS_STATE_ABBR) | CENSUS_STATE_ABBR == "",
                !sf::st_is_empty(geometry)) |>
  dplyr::select(EIN2, geometry) |>
  sf::st_as_sf()

### 12 records

# Transform counties to WGS84

county_transformed <- sf::st_transform(county_tigris, 4326)

# Spatial join with counties

null_county <- sf::st_join(null_geoms, 
                           county_transformed, 
                           join = sf::st_intersects)

# Create state FIPS to abbreviation lookup

state_lookup <- setNames(state_tigris$CENSUS_STATE_ABBR, 
                         state_tigris$CENSUS_STATE_FIPS)

# Add state abbreviations to county-matched records

null_county <- null_county |>
  dplyr::mutate(CENSUS_STATE_ABBR = state_lookup[CENSUS_STATE_FIPS])

# Create EIN to state abbreviation lookup

ein_lookup <- setNames(null_county$CENSUS_STATE_ABBR, 
                       null_county$EIN2)

# Update original dataset with filled state abbreviations

full_sample_int <- full_sample_int |>
  dplyr::mutate(
    CENSUS_STATE_ABBR = ifelse(EIN2 %in% null_county$EIN2,
                               ein_lookup[EIN2], 
                               CENSUS_STATE_ABBR)
  )

# (7.2) - Missing County Information

# Identify records with missing state but valid geometry

null_geoms <- full_sample_int |>
  dplyr::filter(is.na(CENSUS_COUNTY_NAME) | CENSUS_COUNTY_NAME == "",
                !sf::st_is_empty(geometry)) |>
  dplyr::select(EIN2, geometry) |>
  sf::st_as_sf()

### 12 records all with 0 coordinates

# Spatial join with counties

null_county <- sf::st_join(null_geoms, 
                           county_transformed, 
                           join = sf::st_intersects)

# Create EIN to state abbreviation lookup

ein_lookup <- setNames(null_county$CENSUS_COUNTY_NAME, 
                       null_county$EIN2)

# Update original dataset with imputed counties

full_sample_int <- full_sample_int |>
  dplyr::mutate(
    CENSUS_COUNTY_NAME = ifelse(EIN2 %in% null_county$EIN2,
                               ein_lookup[EIN2], 
                               CENSUS_COUNTY_NAME)
  )

## Note: None should be updated because all records have POINT(0 0) coordinates

# Check if all counties are in the sample

sample_county <- full_sample_int |>
  dplyr::select(CENSUS_STATE_ABBR, CENSUS_COUNTY_NAME) |>
  dplyr::distinct()

absent_counties <- data.frame(county_state) |>
  dplyr::filter(! CENSUS_COUNTY_NAME %in% sample_county$CENSUS_COUNTY_NAME) |>
  dplyr::select(CENSUS_STATE_ABBR, CENSUS_COUNTY_NAME)

# Save dataset for use in table creation

data.table::fwrite(absent_counties, "data/intermediate/absent_counties.csv")

### 179 counties are not in the sample

# (7.2) - Missing Congressional District Information. Unnecessary after mapping the missing state information

# Identify records with missing state but valid geometry

null_geoms <- full_sample_int |>
  dplyr::filter(is.na(CENSUS_STATE_ABBR) | CENSUS_STATE_ABBR == "",
                !sf::st_is_empty(geometry)) |>
  dplyr::select(EIN2, geometry) |>
  sf::st_as_sf()

# Spatial join with congressional districts

null_districts <- sf::st_join(null_geoms, 
                              cd_transformed, 
                              join = sf::st_intersects)

# Add state abbreviations to county-matched records

null_districts <- null_districts |>
  dplyr::mutate(CENSUS_STATE_ABBR = state_lookup[CENSUS_STATE_FIPS])

# Create EIN to state abbreviation lookup

ein_lookup <- setNames(null_districts$CENSUS_STATE_ABBR, 
                       null_districts$EIN2)

# Update original dataset with filled state abbreviations

full_sample_int <- full_sample_int |>
  dplyr::mutate(
    CENSUS_STATE_ABBR = ifelse(EIN2 %in% null_county$EIN2,
                               ein_lookup[EIN2], 
                               CENSUS_STATE_ABBR)
  )

# Check that all districts are in the sample

sample_district <- full_sample_int |>
  dplyr::select(CENSUS_STATE_ABBR, NAMELSAD20) |>
  dplyr::distinct()

absent_districts <- data.frame(district_state) |>
  dplyr::filter(! NAMELSAD20 %in% sample_district$NAMELSAD20) |>
  dplyr::select(CENSUS_STATE_ABBR, NAMELSAD20)

print(absent_districts)

### All districts are in the sample but not all counties

# (8) Post process and save intermediate datasets

data.table::fwrite(full_sample_int, "data/intermediate/full_sample.csv")

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
      .default = "Other US Jurisdictions/Unmapped"
    ),
    CONGRESS_DISTRICT_NAME = ifelse(is.na(NAMELSAD20), "Unmapped", NAMELSAD20)
  ) |>
  dplyr::select(
    EIN2,
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
  dplyr::mutate(
    CENSUS_REGION = ifelse(is.na(CENSUS_REGION), "Unmapped", CENSUS_REGION),
    NAMELSAD20 = ifelse(is.na(NAMELSAD20), "Unmapped", NAMELSAD20)
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
sum(full_sample_proc$AT_RISK_NUM) == sum(efile_sample$at_risk)
sum(full_sample_proc$GOVERNMENT_GRANT_DOLLAR_AMOUNT) == total_gvgrnt

data.table::fwrite(full_sample_proc, "data/processed/full_sample_processed.csv")

# TODO

# 1. Explore unclassified NTEE Codes.