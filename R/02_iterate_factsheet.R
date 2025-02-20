library(rmarkdown)
library(stringr)
library(tidyverse)
library(janitor)
library(usdata)

states <- as.character(usdata::state_stats$state)

# create a data frame with parameters and output file names

# Render national table
rmarkdown::render(
  input = "R/02_factsheet.Rmd",
  output_dir = "docs/",
  output_file = "national.html",
  params = list(geography = "US")
)

# Render State Table
for (state in states) {
  cat("Rendering", state, "\n")
  rmarkdown::render(
    input = "R/03_state_tables.Rmd",
    output_dir = "docs/",
    output_file = paste0(gsub(" ", "-", tolower(state)), ".html"),
    params = list(state = state)
  )
}