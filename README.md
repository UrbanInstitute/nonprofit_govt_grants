# nonprofit_govt_grants

This repository contains the replication code, datasets and workflow for a series of HTML factsheets describing the reliance of nonprofit organizations on government grants in the United States.

## Directory Descriptions

### /data
- **raw/**: Contains original, unmodified data files. These should never be altered once added.
    - This directory is not included in the repository due to the size of the files. To download the raw data, run the code in `R/00_data_process.R`
- **intermediate/**: Stores partially processed data files and temporary outputs from processing steps.
    - **qa.xlsx**: Is a master dataset containing all data used in factsheets for quality assurance.
- **processed/**: Contains final, cleaned datasets that are used in the factsheets.
    - **state_factsheets/**: Contains state-level data for each state in the United States, disaggregated by county, district, size and subsector.
    - **state_overviews/**: Contains state-level data for the United States without disaggregations.
    - **full_sample_processed.csv** Contains the records for each return used in the final sample.

### /R
- Contains all R code for data analysis, processing, and visualization. Scripts are numbered sequentially (e.g., 01_clean_data.R, 02_process_data.R) to indicate the order of execution. Helper functions are stored in separate scripts named after their primary function (e.g., calculate_metrics.R, plot_utilities.R).
- Also includes the .Rmd template for the factsheets. 02_national_factsheet.Rmd is the template for the national factsheet, while 03_state_factsheet.Rmd is the template for the state factsheets.

### /docs
- Contains the HTML factsheets generated from the data analysis. Each factsheet is named according to the geography it covers (e.g. national, alabama etc.)