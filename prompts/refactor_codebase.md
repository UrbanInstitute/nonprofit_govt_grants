Refactor this codebase. Before making any changes, read all relevant scripts and propose a refactoring plan for my approval.
1. Extract download logic
Identify all raw file download code and move it to a dedicated R/00_download.R script. Ensure 00_data_process.R calls this script rather than duplicating logic.
2. Refactor for readability
Improve 00_data_process.R, R/01_analysis.R, and R/02_iterate_factsheet.R:

Break up long or complex code blocks
Use consistent naming conventions
Add section comments where logic changes
Ensure each script has a clear, single responsibility

3. Centralize globals and constants
Create R/config.R and move all global variables, constants, and configuration values there. Update all scripts to source config.R rather than defining values inline.
4. Audit helper functions
Review all helper functions across the codebase:

Identify redundant or near-duplicate functions and consolidate
Evaluate performance — flag any functions that could be vectorized, avoid repeated computation, or reduce memory overhead
Assess how functions are structured and called — propose a consistent pattern
Recommend whether helpers should be reorganized across files

Workflow: Propose all changes before writing any code. Flag any refactoring decisions that involve tradeoffs so I can weigh in before you proceed.