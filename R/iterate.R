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

#make a list of all staging links
all_links <- data.frame(msa_identifiers$directory_name) %>% 
  rename(dir = msa_identifiers.directory_name) %>% 
  mutate(link = paste0("https://apps-staging.urban.org/features/spcp-msas-r-markdown/factsheets/",dir,".html"))

write_csv(all_links, "all_staging_links.csv")

dropdown_text <- msa_identifiers %>% 
  mutate(text = paste0("<option value='",directory_name,"'><a href='https://datacatalog.urban.org/dataset/'>",city_state,"</a></option>")) %>% 
  select(text)

write_csv(dropdown_text, "dropdown_text_data_cat.csv")