############################# Script Header ####################################
# Title: Data request for Lily Janiak from the San Francisco Chronicle
# Details: Government grants dependency for arts organizations in Bay Area
# counties
# Date created: 2025-06-10
# Date last edited: 2025-06-12
# Programmer: Thiyaghessan [tpoongundranar@urban.org]
# Raw Data:
# Efile Part I: https://nccs-efile.s3.us-east-1.amazonaws.com/public/efile_v2_0/F9-P01-T00-SUMMARY-2021.CSV
# Efile Part VIII: https://nccs-efile.s3.us-east-1.amazonaws.com/public/efile_v2_0/F9-P08-T00-REVENUE-2021.CSV
# Unified BMF: https://nccsdata.s3.amazonaws.com/harmonized/bmf/unified/BMF_UNIFIED_V1.1.csv
# Details:
# - Percent receiving some government grant
# - Percent of budget from government grants
################################################################################

# libraries
library(tidyverse)
library(data.table)

# Load in data
full_sample <- data.table::fread("data/processed/full_sample_processed.csv")
efile_p01_2021 <- data.table::fread("D:/Urban/NCCS/remote_data/efile/F9-P01-T00-SUMMARY-2021.csv")
unified_bmf <- data.table::fread("D:/Urban/NCCS/remote_data/BMF_UNIFIED_V1.1.csv")
efile_p08_2021 <- data.table::fread("D:/Urban/NCCS/remote_data/efile/F9-P08-T00-REVENUE-2021.csv")

# (1) - Extract Data

# (1.1) - Filter data tool data to bay area arts organizations

# Bay Area counties - https://census.bayareametro.gov/cities-counties
bay_area_counties <- c(
  "Alameda County",
  "Contra Costa County",
  "Marin County",
  "Napa County",
  "San Francisco County",
  "San Mateo County",
  "Santa Clara County",
  "Solano County",
  "Sonoma County"
)

bay_area_arts_orgs <- full_sample |>
  dplyr::filter(
    CENSUS_COUNTY_NAME %in% bay_area_counties,
    SUBSECTOR == "Arts, Culture, and Humanities"
  )

summary(bay_area_arts_orgs)
# 452 orgs. 
# Check if all counties are present:
setdiff(bay_area_counties, unique(bay_area_arts_orgs$CENSUS_COUNTY_NAME))
# Check if only one subsector is present
unique(bay_area_arts_orgs$SUBSECTOR)

# (1.2) - Get most recent revenue data from 2021 e-filed records for bay area arts organizations

bay_area_eins <- unique(bay_area_arts_orgs$EIN2)

efile_p01_bay_area <- efile_p01_2021 |>
  dplyr::filter(EIN2 %in% bay_area_eins,
                TAX_YEAR == 2021) |>
  dplyr::select(EIN2, F9_01_REV_TOT_CY, F9_01_EXP_TOT_CY, RETURN_TIME_STAMP) |>
  dplyr::mutate(RETURN_TIME_STAMP = lubridate::ymd_hms(RETURN_TIME_STAMP)) |>
  dplyr::group_by(EIN2) |>
  dplyr::slice_max(RETURN_TIME_STAMP) |>
  dplyr::ungroup() |>
  dplyr::select(! RETURN_TIME_STAMP) |>
  dplyr::rename(TOTAL_REVENUE = F9_01_REV_TOT_CY,
                TOTAL_FUNC_EXPENSES = F9_01_EXP_TOT_CY)

# (1.3) - E-filed record for government grants and contributions reported
efile_p08_bay_area <- efile_p08_2021 |>
  dplyr::filter(EIN2 %in% bay_area_eins,
                TAX_YEAR == 2021) |>
  dplyr::select(EIN2, F9_08_REV_CONTR_GOVT_GRANT, RETURN_TIME_STAMP) |>
  dplyr::mutate(RETURN_TIME_STAMP = lubridate::ymd_hms(RETURN_TIME_STAMP)) |>
  dplyr::group_by(EIN2) |>
  dplyr::slice_max(RETURN_TIME_STAMP) |>
  dplyr::ungroup() |>
  dplyr::select(! RETURN_TIME_STAMP) |>
  dplyr::mutate(GOVERNMENT_GRANT_RECEIVED = ifelse(F9_08_REV_CONTR_GOVT_GRANT > 0, 1, 0)) |>
  dplyr::select(! F9_08_REV_CONTR_GOVT_GRANT)

# (2) - Compute metrics (transform)

# (2.1) - Percent receiving at least some form of government grants
# (2.2) - Percent of budget from government grants

bay_area_arts_orgs_county_summary <- bay_area_arts_orgs |>
  tidylog::left_join(efile_p01_bay_area) |>
  tidylog::left_join(efile_p08_bay_area) |>
  dplyr::mutate(RECEIVED_GOV_GRANT = ifelse(GOVERNMENT_GRANT_DOLLAR_AMOUNT > 0, 1, 0),
                GOV_GRANT_PERCENT_REVENUE = GOVERNMENT_GRANT_DOLLAR_AMOUNT / TOTAL_REVENUE,
                GOV_GRANT_PERCENT_EXPENSES = GOVERNMENT_GRANT_DOLLAR_AMOUNT / TOTAL_FUNC_EXPENSES) |>
  dplyr::group_by(CENSUS_COUNTY_NAME) |>
  dplyr::summarise(
    "Number of arts, culture, and humanities 990 filers" = dplyr::n(),
    "Number of arts, culture, and humanities 990 filers with government grants" = sum(GOVERNMENT_GRANT_RECEIVED, na.rm = TRUE),
    "Total government grants ($)" = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT, na.rm = TRUE),
    "Mean percentage of revenue from government grants" = scales::percent(mean(GOV_GRANT_PERCENT_REVENUE, na.rm = TRUE))
  ) |>
  dplyr::rename("California County" = CENSUS_COUNTY_NAME)

bay_area_arts_orgs_summary <- bay_area_arts_orgs |>
  tidylog::left_join(efile_p01_bay_area) |>
  tidylog::left_join(efile_p08_bay_area) |>
  dplyr::mutate(RECEIVED_GOV_GRANT = ifelse(GOVERNMENT_GRANT_DOLLAR_AMOUNT > 0, 1, 0),
                GOV_GRANT_PERCENT_REVENUE = GOVERNMENT_GRANT_DOLLAR_AMOUNT / TOTAL_REVENUE,
                GOV_GRANT_PERCENT_EXPENSES = GOVERNMENT_GRANT_DOLLAR_AMOUNT / TOTAL_FUNC_EXPENSES) |>
  dplyr::summarise(
    "Number of arts, culture, and humanities 990 filers" = dplyr::n(),
    "Number of arts, culture, and humanities 990 filers with government grants" = sum(GOVERNMENT_GRANT_RECEIVED, na.rm = TRUE),
    "Total government grants ($)" = sum(GOVERNMENT_GRANT_DOLLAR_AMOUNT, na.rm = TRUE),
    "Mean percentage of revenue from government grants" = scales::percent(mean(GOV_GRANT_PERCENT_REVENUE, na.rm = TRUE))
  ) |>
  dplyr::mutate("California County" = "All Bay Area Counties")


bay_area_arts_orgs_processed <- bay_area_arts_orgs_county_summary |>
  dplyr::bind_rows(bay_area_arts_orgs_summary)

# (3) - Create excel workbook (Load)

writexl::write_xlsx(
  list("Data" = bay_area_arts_orgs_processed),
  "data/processed/data_requests/sf_chronicle-bay_area_arts_orgs_processed-06122025.xlsx"
)
