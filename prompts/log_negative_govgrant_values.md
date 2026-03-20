Issue:
Validation check in R/validate_processed_data.R lines (71-79) fails when R/run_pipeline.R is run

Context:
Negative government grant values are not invalid, but it's important for this information to be recorded somewhere

Solution:
- Do not stop code execution if check fails
- Output a message to the console indicating the number of records with negative government grant values
- Add a sheet to the qa column with the following structure:
	- name: Notable government grant values
	- columns: ein | tax_year | nonprofit name | total revenue | total expenses | government grant dollars | notes
	- In the notes column, these should be specified as having negative values

Create an implementation plan and present it to me before proceeding