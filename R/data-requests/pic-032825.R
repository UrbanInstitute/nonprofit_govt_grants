# Script Header
# Title: Data request submitted by Alan Berube of the Policy Innovation Centre
# Date created: 2020-03-28
# Date last modiiied: 2020-03-28
# Programmer: Thiyaghessan[tpoongundranar@urban.org]
# Details: Request for data from individual electronically filed 990s across the US, and no longer just San Diego, for all 501©3 Public Charities in Tax Year 2021 with the following financial metrics:
# •	Total dollar value of government grants
# •	Total revenue
# •	Total expenses
# •	Total assets
# 
# And the following demographic metrics:
#   •	Organization name
# •	Organization address with:
#   o	State
# o	County
# •	Organization Subsector
# •	EIN


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
source("R/format_ein.R") # Function to format ein to EIN 2
source("R/profit_margin.R") # Function to calculate profit margin

# (1) -  Load in Data with the necessary columns and datatypes

# (1.1) - BMF Data

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
# (1.2) - Tigris data. We want to ensure our sample is complete or at the very least aware of the presence of missing states, counties and/or districts.

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

# (1.3) Efile data

efile_cols <- list(
  character = c(
    "EIN2", # EIN formatted to 9 digits like EIN-XX-XXXXXXX
    "TAX_YEAR", # Tax year of the return
    "RETURN_TYPE", # Type of return (990 or 990EZ)
    "OBJECTID", # Unique identifier
    "URL", # URL to the raw return
    "F9_00_EXEMPT_STAT_501C3_X", # Indicates if the organization is a 501c3 public charity (since only public charities file form 990, private foundations file form 990PF)
    "RETURN_TIME_STAMP", # Date and time return was filed
    "F9_00_ORG_NAME_L1", # Name of the filing organization
    "F9_00_ORG_ADDR_L1" # Address of the filing organization
  ),
  numeric = c(
    "F9_08_REV_CONTR_GOVT_GRANT",
    # Total government grants - Part 8
    "F9_01_EXP_TOT_CY",
    # Total revenue - Part 1
    "F9_01_REV_TOT_CY",
    # Total expenses - Part 1
    "F9_01_NAFB_ASSET_TOT_EOY" # Total assets - Part 1
  ),
  logical = c(
    "RETURN_PARTIAL_X", # Indicates if return is a partial return
    "RETURN_GROUP_X", # Indicates if return is a group return
    "RETURN_AMENDED_X" # Indicates if return is an amended return
  )
)

efile_21_hd_raw <- data.table::fread("data/raw/efile_hd_2021_0225.csv", select = efile_cols)
efile_21_p01_raw <- data.table::fread("data/raw/efile_p01_2021_0225.csv", select = efile_cols)
efile_21_p08_raw <- data.table::fread("data/raw/efile_p08_2021_0225.csv", select = efile_cols)

# (2) - Create the sample dataset from the efile data

# (2.1) - Exclude foreign nonprofits

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

# (2.2) - Only include 501c3 public charities

eins_501c3 <- efile_21_hd_raw |>
  dplyr::filter(F9_00_EXEMPT_STAT_501C3_X == "X") |>
  dplyr::select(EIN2) |>
  dplyr::distinct() |>
  dplyr::pull(EIN2)

length(eins_501c3)

# 392, 704 501c3 public charities

# (2.3) - Filter form 990 records from 501c3 public charities, filed in tax year 2021, and not from the list from foreign EINs

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
  dplyr::group_by(EIN2) |>
  dplyr::slice_max(order_by = RETURN_TIME_STAMP)

### 306 records: 4 duplicates discarded

nrow(efile_grp) == length(group_eins) # TRUE
efile_21_p08 <- dplyr::bind_rows(efile_nogrp, efile_grp)

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
  dplyr::group_by(EIN2) |>
  dplyr::slice_max(order_by = RETURN_TIME_STAMP)

### 3,867 records: 3,511 duplicates discarded

nrow(efile_amended) == length(amended_eins) # TRUE
efile_21_p08 <- dplyr::bind_rows(efile_noamend, efile_amended)

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

# (3) - Wrangle Data

# (3.1) - Wrangle BMF Data

bmf_sample <- unified_bmf |>
  dplyr::filter(NCCS_LEVEL_1 == "501C3 CHARITY") |>
  dplyr::mutate(
    SUBSECTOR = substr(NTEEV2, 1, 3),
    GEOID_TRACT_10 = substr(CENSUS_BLOCK_FIPS, 1, 11),
    EIN2 = format_ein(EIN2, to = "n")
  ) |>
  dplyr::mutate(
    SUBSECTOR = ifelse(SUBSECTOR == "", "UNU", SUBSECTOR),
    EIN2 = format_ein(EIN2, to = "id")
  )

nrow(bmf_sample) == nrow(unified_bmf[unified_bmf$NCCS_LEVEL_1 == "501C3 CHARITY"]) #TRUE

## Map BMF coordinates to Congressional districts

bmf_sample <- bmf_sample |>
  sf::st_as_sf(coords = c("LONGITUDE", "LATITUDE"), crs = 4326)

bmf_sample <- sf::st_join(bmf_sample, 
                          cd_transformed, 
                          join = sf::st_intersects)

## Optional: To save memory

rm(unified_bmf)
gc()

# (3.2) Wrangle efile data

## Merge all 3 e-file datasets. Left join to Part VIII since that contains the government grant information

efile_sample <- efile_21_p08 |>
  dplyr::select(! RETURN_TIME_STAMP) |>
  dplyr::filter(!is.na(F9_08_REV_CONTR_GOVT_GRANT),
                F9_08_REV_CONTR_GOVT_GRANT != 0) |>
  tidylog::left_join(efile_21_p01_raw) |>
  tidylog::left_join(efile_21_hd_raw)

nrow(efile_sample) == numrec_w_gvgrnt 

### No new records added

sum(efile_sample$F9_08_REV_CONTR_GOVT_GRANT) == total_gvgrnt  
### Total remains the same

## Optional: To save memory

rm(efile_21_p08_raw, 
   efile_21_p01_raw,
   efile_21_hd_raw,
   efile_21_p08)
gc()

## Wrangle efile data

efile_sample <- efile_sample |>
  dplyr::select(
    "EIN2",
    "F9_08_REV_CONTR_GOVT_GRANT",
    "F9_01_EXP_TOT_CY",
    "F9_01_REV_TOT_CY",
    "F9_01_NAFB_ASSET_TOT_EOY",
    "F9_00_ORG_NAME_L1",
    "F9_00_ORG_ADDR_L1"
  )

nrow(efile_sample) == numrec_w_gvgrnt #TRUE
sum(efile_sample$F9_08_REV_CONTR_GOVT_GRANT) == total_gvgrnt  # TRUE

# (5) - Merge with the geographic data from the Unified BMF

full_sample_int <- efile_sample |>
  tidylog::left_join(
    bmf_sample <- bmf_sample |>
      dplyr::arrange(ORG_YEAR_LAST),
    by = c("EIN2" = "EIN2"),
    multiple = "last"
  )

sum(full_sample_int$F9_08_REV_CONTR_GOVT_GRANT) == total_gvgrnt # TRUE
nrow(full_sample_int) == numrec_w_gvgrnt # TRUE


# (6) - Geographic post processing. Map null geographies with coordinates but without state, county, or district information. Ensure sample contains all possible state and county/district combinations.

# (6.1) - Missing state information

# Identify records with missing state but valid geometry

null_geoms <- full_sample_int |>
  dplyr::filter(is.na(CENSUS_STATE_ABBR) | CENSUS_STATE_ABBR == "",
                !sf::st_is_empty(geometry)) |>
  dplyr::select(EIN2, geometry) |>
  sf::st_as_sf()

### 44 records

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

### 44 records all with 0 coordinates

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

### 179 counties are not in the sample

# (7.2) - Missing Congressional District Information. Unnecessary after mapping the missing state information

# Identify records with missing state but valid geometry

null_geoms <- full_sample_int |>
  dplyr::filter(is.na(CENSUS_STATE_ABBR) | CENSUS_STATE_ABBR == "",
                !sf::st_is_empty(geometry)) |>
  dplyr::select(EIN2, geometry) |>
  sf::st_as_sf()

# 12 records with missing observations

#Spatial join with congressional districts

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

# (7) Post process and save output dataset

names(full_sample_int)

results_df <- full_sample_int |>
  dplyr::select(
    EIN2,
    F9_08_REV_CONTR_GOVT_GRANT,
    F9_01_EXP_TOT_CY,
    F9_01_REV_TOT_CY,
    F9_01_NAFB_ASSET_TOT_EOY,
    F9_00_ORG_NAME_L1,
    F9_00_ORG_ADDR_L1,
    CENSUS_STATE_ABBR,
    CENSUS_COUNTY_NAME,
    NAMELSAD20,
    SUBSECTOR
  ) |>
  dplyr::mutate(
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
    NAMELSAD20 = ifelse(is.na(NAMELSAD20), "Unmapped", NAMELSAD20)
  ) |>
  dplyr::rename(
    CONGRESS_DISTRICT_NAME = NAMELSAD20,
    GOVERNMENT_GRANT_DOLLAR_AMOUNT = F9_08_REV_CONTR_GOVT_GRANT
  ) |>
  dplyr::mutate(
    CONGRESS_DISTRICT_NAME = dplyr::case_when(
      CONGRESS_DISTRICT_NAME == "Congressional District 1" ~ "1st Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 2" ~ "2nd Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 3" ~ "3rd Congressional district", 
      CONGRESS_DISTRICT_NAME == "Congressional District 4" ~ "4th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 5" ~ "5th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 6" ~ "6th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 7" ~ "7th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 8" ~ "8th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 9" ~ "9th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 10" ~ "10th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 11" ~ "11th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 12" ~ "12th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 13" ~ "13th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 14" ~ "14th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 15" ~ "15th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 16" ~ "16th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 17" ~ "17th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 18" ~ "18th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 19" ~ "19th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 20" ~ "20th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 21" ~ "21st Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 22" ~ "22nd Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 23" ~ "23rd Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 24" ~ "24th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 25" ~ "25th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 26" ~ "26th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 27" ~ "27th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 28" ~ "28th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 29" ~ "29th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 30" ~ "30th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 31" ~ "31st Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 32" ~ "32nd Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 33" ~ "33rd Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 34" ~ "34th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 35" ~ "35th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 36" ~ "36th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 37" ~ "37th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 38" ~ "38th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 39" ~ "39th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 40" ~ "40th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 41" ~ "41st Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 42" ~ "42nd Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 43" ~ "43rd Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 44" ~ "44th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 45" ~ "45th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 46" ~ "46th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 47" ~ "47th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 48" ~ "48th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 49" ~ "49th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 50" ~ "50th Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 51" ~ "51st Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District 52" ~ "52nd Congressional district",
      CONGRESS_DISTRICT_NAME == "Congressional District (at Large)" ~ "Congressional district (at large)",
      CONGRESS_DISTRICT_NAME == "Delegate District (at Large)" ~ "Delegate district (at large)",
      CONGRESS_DISTRICT_NAME == "Resident Commissioner District (at Large)" ~ "Resident commissioner district (at large)",
      CONGRESS_DISTRICT_NAME == "Unmapped" ~ "Unmapped",
      TRUE ~ CONGRESS_DISTRICT_NAME
    ),
    SUBSECTOR = dplyr::case_when(
      SUBSECTOR == "Arts, Culture, and Humanities" ~ "Arts, culture, and humanities",
      SUBSECTOR == "Education (Excluding Universities)" ~ "Education",
      SUBSECTOR == "Environment and Animals" ~ "Environment and animals",
      SUBSECTOR == "Health (Excluding Hospitals)" ~ "Health",
      SUBSECTOR == "Human Services" ~ "Human services",
      SUBSECTOR == "Hospitals" ~ "Hospitals",
      SUBSECTOR == "International, Foreign Affairs" ~ "International, foreign affairs",
      SUBSECTOR == "Public, Societal Benefit" ~ "Public, societal benefit",
      SUBSECTOR == "Religion Related" ~ "Religion-related",
      SUBSECTOR == "Mutual/Membership Benefit" ~ "Mutual/membership benefit",
      SUBSECTOR == "Universities" ~ "Universities",
      SUBSECTOR == "Unclassified" ~ "Unclassified",
      TRUE ~ SUBSECTOR
    )
  ) 

results_df <- results_df |>
  dplyr::rename(
    "Total dollar value of government grants" = GOVERNMENT_GRANT_DOLLAR_AMOUNT, "Total revenue" = F9_01_REV_TOT_CY,
    "Total expenses" = F9_01_EXP_TOT_CY, 
    "Total assets" = F9_01_NAFB_ASSET_TOT_EOY,
    "Organization name" = F9_00_ORG_NAME_L1,
    "Organization address" = F9_00_ORG_ADDR_L1,
    "Organization Subsector" = SUBSECTOR,
    "EIN" = EIN2,
    "State" = CENSUS_STATE_NAME,
    "County" = CENSUS_COUNTY_NAME,
    "Congressional District" = CONGRESS_DISTRICT_NAME,
    "State Abbreviation" = CENSUS_STATE_ABBR
  )

data.table::fwrite(results_df, "data/data-requests/pic-032825.csv")
