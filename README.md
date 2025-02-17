# nonprofit_govt_grants

This repository contains the replication code, datasets and workflow for a series of HTML factsheets describing the reliance of nonprofit organizations on government grants in the United States.

## Directory Descriptions

### /data
- **raw/**: Contains original, unmodified data files. These should never be altered once added.
- **intermediate/**: Stores partially processed data files and temporary outputs from processing steps.
- **processed/**: Contains final, cleaned datasets that are used in the factsheets.

### /R
- Contains all R code for data analysis, processing, and visualization. Scripts are numbered sequentially (e.g., 01_clean_data.R, 02_process_data.R) to indicate the order of execution. Helper functions are stored in separate scripts named after their primary function (e.g., calculate_metrics.R, plot_utilities.R).