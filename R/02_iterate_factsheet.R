# Script Header
# Title: Federal Funding Freeze Blog Post
# Date created: 2025-02-03
# Date last modified: 2025-02-20
# Description: This script contains code to iterate across the national_factsheet.Rmd and state_factsheet.Rmd files to create HTML fact sheets for the US and each state respectively.

# Load Packages
library(rmarkdown)
library(stringr)
library(tidyverse)
library(janitor)
library(usdata)

# Names of states
states <- as.character(usdata::state_stats$state)

# (1) - Render national table
rmarkdown::render(
  input = "R/national_factsheet.Rmd",
  output_dir = "docs/",
  output_file = "national.html",
  params = list(geography = "United States")
)

# (2) - Iterate and Render State Tables

## Only if you want to debug
test_states <- c("District of Columbia", "Alaska", "California" )

for (state in states) {
  cat("Rendering", state, "\n")
  rmarkdown::render(
    input = "R/state_factsheet.Rmd",
    output_dir = "docs/",
    output_file = paste0(gsub(" ", "-", tolower(state)), ".html"),
    params = list(state = state)
  )
}
