# Sample for spatial joins
library(tigris)
library(sf)

# Load required libraries
library(tigris)
library(sf)
library(dplyr)

# Convert BMF data to an sf object
ma_bmf <- unified_bmf |>
  dplyr::filter(CENSUS_STATE_ABBR == "MA")  # Filter for Massachusetts data

bmf_sf <- ma_bmf |>
  st_as_sf(coords = c("LONGITUDE", "LATITUDE"), 
           crs = 4326)  # WGS 84 coordinate system

# Get congressional districts
# Note: By default, this gets the most current districts
cd <- congressional_districts()
cd_transformed <- st_transform(cd, 4326)

# Perform spatial join
results <- st_join(bmf_sf, cd_transformed, join = st_intersects)

# Clean up results to keep only relevant columns
# Adjust the select statement based on which columns you want to keep
final_results <- results |>
  dplyr::select(EIN2, NAMELSAD20)  # NAMELSAD contains the district name

ma_bmf_cd <- ma_bmf |>
  tidylog::left_join(final_results, by = "EIN2")

# efile data
efile_21_raw <- data.table::fread("https://nccs-efile.s3.us-east-1.amazonaws.com/public/v2025/F9-P08-T00-REVENUE-2021.csv") 

efile_21_new <- efile_21_raw |>
  dplyr::filter(
    TAX_YEAR == 2021
  ) |>
  dplyr::select(F9_08_REV_CONTR_GOVT_GRANT, 
                F9_08_REV_TOT_TOT,
                EIN2)

ma_bmf_cd <- ma_bmf_cd |>
  dplyr::filter(
    NCCS_LEVEL_1 == "501C3 CHARITY"
  ) |>
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
    ) # Add census regions based on states
  ) |>
  dplyr::mutate(SUBSECTOR = ifelse(SUBSECTOR == "", "UNU", SUBSECTOR))


newein2 <- ma_bmf_cd$EIN2 %>% 
  format_ein( to="n"  ) %>%
  format_ein( to="id" )

ma_bmf_cd$EIN2 <- newein2

ma_full <- efile_21_new |>
  tidylog::left_join(ma_bmf_cd, by = "EIN2")


table(ma_full$SUBSECTOR, ma_full$NAMELSAD20)              

tapply( 
  as.numeric(ma_full$F9_08_REV_CONTR_GOVT_GRANT), 
  list(ma_full$SUBSECTOR,ma_full$NAMELSAD20), 
  FUN=sum, na.rm=T ) |> 
  View()







# Write results EIN2# Write results to a new file
write.csv(final_results, "bmf_with_districts.csv", row.names = FALSE)