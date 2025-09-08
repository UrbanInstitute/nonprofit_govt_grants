# Script Header
# Title: Federal Funding Freeze Blog Post
# Date created: 2025-02-03
# Date last modified: 2025-03-07
# Description: This script contains code to analyze data for HTML fact sheets 
# on nonprofits's fiscal sustainability and reliance on government grants for 
# Tax Year 2021. It creates data.frames for fact sheets for each disaggregation.
# Detailed Description
# (1) - Create national level tables
# (2) - Create state level tables
# (3) - Create quality assurance tables for internal team viewing (optional)

# Packages
library(tidyverse)
library(data.table)
library(purrr)
library(tidyr)
library(rlang)
library(janitor)
library(writexl)

# Create output directories
dir.create("data/processed/state_factsheets", showWarnings = FALSE)
dir.create("data/processed/state_overviews", showWarnings = FALSE)

# Helper scripts
source("R/summarise_data.R")
source("R/retrieve_missing_counties.R")

# Load in data
full_sample_proc <- data.table::fread("data/processed/full_sample_processed_v1.0.csv")
absent_counties <- data.table::fread("data/intermediate/absent_counties.csv")

# (1) - National Summaries. 

# (1.1) - Create three tables, disaggregated by  state, subsector and size. Add the totals at the very end.

national <- summarize_nonprofit_data(full_sample_proc)

national_bystate <- dplyr::mutate(national, State = "United States") |>
  dplyr::bind_rows(
    summarize_nonprofit_data(
      full_sample_proc,
      group_var = "CENSUS_STATE_NAME",
      group_var_rename = "State"
    )
  ) |>
  dplyr::relocate(State)

national_bysize <- summarize_nonprofit_data(full_sample_proc,
                                            group_var = "EXPENSE_CATEGORY",
                                            group_var_rename = "Size") |>
  dplyr::bind_rows(dplyr::mutate(national, Size = "Total"))

national_bysubsector <- summarize_nonprofit_data(full_sample_proc,
                                                 group_var = "SUBSECTOR",
                                                 group_var_rename = "Subsector") |> dplyr::bind_rows(dplyr::mutate(national, Subsector = "Total"))

# (1.2) - Save datasets to the processed/ folder

datasets <- list(
  "national_bystate" = national_bystate,
  "national_bysize" = national_bysize,
  "national_bysubsector" = national_bysubsector
)

purrr::walk2(
  datasets,
  names(datasets),
  function(df, name) {
    data.table::fwrite(df, paste0("data/processed/", name, ".csv"))
  }
)

# (1.3) - Create an .xlsx spreadsheet for the team to view (optional)

writexl::write_xlsx(list(
  "National" = national,
  "State" = national_bystate,
  "Size" = national_bysize,
  "Subsector" = national_bysubsector
), path = "data/processed/national_overview.xlsx")

# (2) - Create state tables for each state.

states <- as.character(usdata::state_stats$state)

for (state in states) {
  
  cat("Processing ", state, "\n")
  
  state_sample <- full_sample_proc |>
    dplyr::filter(CENSUS_STATE_NAME == state)
  
  # Retrieve counties with 0 nonprofits receiving government grants for each state.
  missing_county <- retrieve_missing_counties(absent_counties, state = state)
  
  # Create aggregated state level data
  state_overall <- summarize_nonprofit_data(state_sample)
  
  # Disaggregate by county, congressional district, size, and subsector. Append totals at the end.
  state_bycounty <- dplyr::mutate(national, Geography = "United States") |>
    dplyr::bind_rows(dplyr::mutate(state_overall, Geography = state)) |>
    dplyr::bind_rows(
      summarize_nonprofit_data(state_sample,
                               group_var = "CENSUS_COUNTY_NAME",
                               group_var_rename = "Geography")
    ) |>
    dplyr::relocate(Geography)
    
  state_bydistrict <- dplyr::mutate(national, Geography = "United States") |>
    dplyr::bind_rows(dplyr::mutate(state_overall, Geography = state)) |>
    dplyr::bind_rows(
      summarize_nonprofit_data(state_sample,
                               group_var = "CONGRESS_DISTRICT_NAME",
                               group_var_rename = "Geography") 
    ) |>
    dplyr::relocate(Geography)
  
  state_bysize <- summarize_nonprofit_data(state_sample,
                                           group_var = "EXPENSE_CATEGORY",
                                           group_var_rename = "Size") |>
    dplyr::bind_rows(dplyr::mutate(state_overall, Size = "Total"))
  
  state_bysubsector <- summarize_nonprofit_data(state_sample,
                                                group_var = "SUBSECTOR",
                                                group_var_rename = "Subsector") |>
    dplyr::bind_rows(dplyr::mutate(state_overall, Subsector = "Total"))
  
  # Save datasets to the processed/ folder
  datasets <- list(
    "_bycounty" = state_bycounty,
    "_bydistrict" = state_bydistrict,
    "_bysize" = state_bysize,
    "_bysubsector" = state_bysubsector
  )
  
  purrr::walk2(datasets, names(datasets), function(df, name) {
    data.table::fwrite(df, paste0("data/processed/state_factsheets/", 
                                  gsub(" ", "-", tolower(state)), 
                                  name, 
                                  ".csv"))
  })
  
  # Save an .xlsx spreadsheet for the team to view (optional)
  writexl::write_xlsx(
    list(
      "Overall" = state_overall,
      "County" = state_bycounty,
      "Congressional District" = state_bydistrict,
      "Size" = state_bysize,
      "Subsector" = state_bysubsector
    ),
    path = paste0("data/processed/state_overviews/", 
                  gsub(" ", "-", tolower(state)), 
                  "_overview.xlsx")
  )
  
}

# (3) - Quality Assurance QA Dataset. This dataset is used to provide teams with an overview of all the data. These are not used for external publication.

# (3.1) - Create Quality Assurance dataset with second state column to aggregate all observations by state.

qa <- full_sample_proc |>
  dplyr::mutate(state_name = CENSUS_STATE_NAME)

# (3.2) - National Level Quality Assurance

qa_national <- summarize_nonprofit_data(qa) |>
  dplyr::mutate(CENSUS_STATE_NAME = "Total")
  

# (3.3) - QA by state

qa_state <- summarize_nonprofit_data(qa,
                                     group_var = "state_name",
                                     group_var_rename = "State",
                                     qa = TRUE) |>
  dplyr::bind_rows(dplyr::mutate(qa_national, State = "Total"))

# (3.4) - QA by Congressional District

qa_district <- summarize_nonprofit_data(qa,
                                        group_var = "CONGRESS_DISTRICT_NAME",
                                        group_var_rename = "Congressional District",
                                        qa = TRUE)  |>
  dplyr::bind_rows(dplyr::mutate(qa_national, State = "Total"))

# (3.5) - QA By County

## Retrieve missing counties with zero nonprofits receiving government grants
missing_counties <- retrieve_missing_counties(absent_counties)
  

qa_county <- summarize_nonprofit_data(qa,
                                      group_var = "CENSUS_COUNTY_NAME",
                                      group_var_rename = "County",
                                      qa = TRUE)  |>
  dplyr::bind_rows(missing_counties) |>
  dplyr::bind_rows(dplyr::mutate(qa_national, County = "Total"))

## Check all counties are unique

qa_county |>
  dplyr::select(CENSUS_STATE_NAME, County) |>
  unique() |>
  nrow() == nrow(qa_county)


# (3.6) - QA by size (expense category)

qa_size <- summarize_nonprofit_data(qa,
                                    group_var = "EXPENSE_CATEGORY",
                                    group_var_rename = "Size",
                                    qa = TRUE)  |>
  dplyr::bind_rows(dplyr::mutate(qa_national, Size = "Total"))

# (3.7) - QA by subsector

qa_subsector <- summarize_nonprofit_data(full_sample_proc,
                                               group_var = "SUBSECTOR",
                                               group_var_rename = "Subsector",
                                               qa = TRUE)  |>
  dplyr::bind_rows(dplyr::mutate(qa_national, Subsector = "Total"))

# (3.8) - Save QA datasets to the intermediate/ folder as an .xlsx

writexl::write_xlsx(list(
  "State" = qa_state,
  "County" = qa_county,
  "Congressional District" = qa_district,
  "Size" = qa_size,
  "Subsector" = qa_subsector
), path = "data/intermediate/qa.xlsx")