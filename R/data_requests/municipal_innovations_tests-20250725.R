# Testing Script to validate code

# Mapping IRS Activity Codes to Policy Areas

ntee_irs <- readr::read_csv("data/data_requests/municipal_innovations/ntee_irs.csv")
policy_ntee_map <- readr::read_csv("data/data_requests/municipal_innovations/ntee_codes.csv")

policy_area_map <- policy_ntee_map |>
  dplyr::select(`NTEE-CC`, `Policy Area`) |>
  dplyr::rename("NTEE_IRS" = `NTEE-CC`)

ntee_irs_policy_area <- ntee_irs |>
  tidylog::left_join(policy_area_map)


readr::write_csv(
  ntee_irs_policy_area,
  "data/data_requests/municipal_innovations/ntee_irs_policy_area.csv"
)


# Atlanta - Nonprofits for manual inspection

# 1: Map bmf nonprofits to atlanta metro
ga_places <- tigris::places(state = "GA")
atlanta_geo <- ga_places |>
  dplyr::filter(NAME == "Atlanta")

atlanta_bmf <- sf::st_join(bmf_sf, atlanta_geo, join = sf::st_intersects) |>
  dplyr::filter(! is.na(NAME)) |>
  dplyr::rename(CITY = NAME)

policy_areas <- policy_ntee_map |>
  dplyr::select(NTEEV2, `Policy Area`) |>
  dplyr::mutate(NTEEV2 = sapply(NTEEV2, convert_format)) |>
  dplyr::mutate(NTEEV2 =  gsub("-PA", "-PS", NTEEV2))

atlanta_metrics <- atlanta_bmf |>
  dplyr::select(EIN2, CITY, NTEE_IRS, ORG_NAME_CURRENT) |>
  dplyr::mutate(NTEEV2 = sapply(NTEE_IRS, convert_ntee_to_v2)) |>
  tidylog::left_join(nonprofit_financial_metrics) |>
  tidylog::left_join(policy_areas) |>
  dplyr::mutate(`Policy Area` = ifelse(is.na(`Policy Area`), "Unmapped", `Policy Area`))

readr::write_csv(atlanta_metrics, "data/data_requests/municipal_innovations/atlanta_policy_area_mappings-20250727.csv")

# 3: Testing NTEE conversions
ntee_irs_test <- policy_ntee_map$`NTEE-CC`
nteev2_test <- policy_areas$NTEEV2

test_that("NTEE is being converted to NTEEV2 correctly", {
  actual_output <- sapply(ntee_irs_test, convert_ntee_to_v2) |> unlist()
  expect_equal(actual_output, nteev2_test, check.attributes = FALSE)
})
