# Angus Watters
# Join StateMod and DDR data
# 01/24/2022

library(tidyverse)
library(readr)
library(stringr)
library(lubridate)
library(readxl)
library(progress)
library(sf)
library(logger)


# read Statemod node data
statemod  <- readr::read_csv("data/statemod/nodes/statemod_node_data.csv")

# length(unique(statemod2$node_id))

# monthly district data
statemod_district_month <- statemod %>% 
  group_by(district, date) %>% 
  summarize(
    demand     = sum(demand, na.rm = T),
    supply     = sum(supply, na.rm = T),
    supply_dir = sum(sup_direct_flow, na.rm = T),
    short      = sum(shortage, na.rm = T),
    short_dir  = sum(shortage_direct, na.rm = T)
  ) %>% 
  mutate(
    aug_supply         = supply - supply_dir,
    aug_supply_pct_dem = round((aug_supply/demand)*100, 3),
    short_pct_dem      = round((short/demand)*100, 3),
    short_dir_pct_dem  = round((short_dir/demand)*100, 3)
  ) %>%
  mutate(
    short_pct_dem = case_when(
      is.nan(short_pct_dem) ~ 0,
      TRUE ~ short_pct_dem
    ),
    short_dir_pct_dem = case_when(
      is.nan(short_dir_pct_dem) ~ 0,
      TRUE ~ short_dir_pct_dem
    ),
    aug_supply_pct_dem = case_when(
      is.nan(aug_supply_pct_dem) ~ 0,
      TRUE ~ aug_supply_pct_dem
    ),
  ) %>% 
  ungroup() %>% 
  mutate(
    year = as.numeric(as.character(lfstat::water_year(date))),
  ) %>%
  relocate(date, year, district,  demand, supply, supply_dir, aug_supply, short, short_dir, aug_supply_pct_dem, short_pct_dem, short_dir_pct_dem)

# replace Infinite w/ 0
is.na(statemod_district_month) <- sapply(statemod_district_month, is.infinite)
statemod_district_month[is.na(statemod_district_month)] <- 0


# save monthly data
write.csv(statemod_district_month, "data/statemod/districts/statemod_district_month.csv", row.names = F)

# yearly district data (water year)
statemod_district_year <- statemod_district_month %>% 
  group_by(district, year) %>% 
  summarize(
    demand     = sum(demand, na.rm = T),
    supply     = sum(supply, na.rm = T),
    supply_dir = sum(supply_dir, na.rm = T),
    aug_supply = sum(aug_supply, na.rm = T),
    short      = sum(short, na.rm = T),
    short_dir  = sum(short_dir, na.rm = T)
  ) %>% 
  mutate(
    aug_supply_pct_dem   = round((aug_supply/demand)*100, 3),
    short_pct_dem        = round((short/demand)*100, 3),
    short_dir_pct_dem    = round((short_dir/demand)*100, 3)
  ) %>%
  mutate(
    year              = as.numeric(as.character(year)),
    short_pct_dem     = case_when(
      is.nan(short_pct_dem) ~ 0,
      TRUE ~ short_pct_dem
    ),
    short_dir_pct_dem = case_when(
      is.nan(short_dir_pct_dem) ~ 0,
      TRUE ~ short_dir_pct_dem
    ),
    aug_supply_pct_dem = case_when(
      is.nan(aug_supply_pct_dem) ~ 0,
      TRUE ~ aug_supply_pct_dem
    )
  ) %>%
  dplyr::relocate(district, year, demand, supply, supply_dir, aug_supply, short, short_dir, aug_supply_pct_dem, short_pct_dem, short_dir_pct_dem) %>% 
  ungroup()

# replace Infinite w/ 0
is.na(statemod_district_year) <- sapply(statemod_district_year, is.infinite)
statemod_district_year[is.na(statemod_district_year)] <- 0

# save yearly data
write.csv(statemod_district_year, "data/statemod/districts/statemod_district_year.csv", row.names = F)













