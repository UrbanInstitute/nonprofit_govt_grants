# Script Header
# Title: Federal Funding Freeze Blog Post
# Date created: 2025-02-03
# Date last modified: 2025-02-20
# Description: This script iterates across the national_factsheet.Rmd and
# state_factsheet.Rmd files to create HTML fact sheets for the US and each
# state respectively.

# ==============================================================================
# PACKAGES
# ==============================================================================

library(rmarkdown)
library(stringr)
library(tidyverse)
library(janitor)
library(usdata)

# ==============================================================================
# CONFIGURATION
# ==============================================================================

source("R/config.R")

#' Render a factsheet from an Rmd template
render_factsheet <- function(template, output_dir, output_file, params) {
  rmarkdown::render(
    input = template,
    output_dir = output_dir,
    output_file = output_file,
    params = params
  )
}

# ==============================================================================
# (1) RENDER NATIONAL FACTSHEET
# ==============================================================================

render_factsheet(
  template = "R/national_factsheet.Rmd",
  output_dir = DIR_DOCS,
  output_file = "national.html",
  params = list(geography = "United States")
)

# ==============================================================================
# (2) RENDER STATE FACTSHEETS
# ==============================================================================

## Only if you want to debug
# test_states <- c("District of Columbia", "Alaska", "California")

for (state in STATE_NAMES) {
  cat("Rendering", state, "\n")
  render_factsheet(
    template = "R/state_factsheet.Rmd",
    output_dir = DIR_DOCS,
    output_file = paste0(gsub(" ", "-", tolower(state)), ".html"),
    params = list(state = state)
  )
}
