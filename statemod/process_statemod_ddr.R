# Angus Watters
# Join StateMod and DDR data
# 01/24/2022

remove(list = ls())

library(tidyverse)
library(readr)
library(stringr)
library(lubridate)
library(readxl)
library(progress)
library(sf)
library(logger)


# StateMod files
statemod  <- readRDS("data/statemod/nodes/statemod_node_data.rds")
# statemod  <- readr::read_csv("data/statemod/nodes/statemod_node_data.csv")

# DDR file
ddr       <- readRDS("data/ddr/outputs/ddr.rds") 
  # mutate(district = substr(node_id, 1, 2))

# manual change some node IDs to include "_I" node info
ddr <- ddr  %>% 
        mutate(
          node_id = case_when(
            node_id ==  "4000751"  ~ "4000751_I",
            node_id ==  "4000549"  ~ "4000549_I",
            node_id ==  "6000670"  ~ "6000670_I",
            node_id ==  "6000672"  ~ "6000672_I",
            node_id ==  "6000707"  ~ "6000707_I",
            node_id ==  "6200560"  ~ "6200560_I",
            !node_id %in% c("4000549", "4000751", "6000670", "6000672", "6000707", "6200560") ~ node_id
          )
        )

# number of unique node IDs in StateMod data 
nodes         <- unique(statemod$node_id)  
length(unique(ddr$node_id))
# filter DDR data only to nodes in StateMod dataset
indiv_decrees <- ddr %>%
  filter(node_id %in% nodes) 
length(unique(indiv_decrees$node_id))

# tmp <- statemod %>% 
#   filter(!node_id %in% unique(indiv_decrees$node_id))
# length(unique(tmp$node_id))
# 0200810
# gsub(
#   ".*0100687.*",
#   "0100687",
#   ddr$node_id
#   )
# Convert Cubic feet/sec to AF/day - (1 cfs = 1.9835 af/day)
af_per_cfs    <- 1.9835 

cols_to_check <- c("demand", "supply", "shortage")

# Join statemod data w/ individual node water rights
statemod_ddr  <- left_join(
                          statemod,
                          indiv_decrees, 
                          by = "node_id"
                          ) %>%
  janitor::clean_names() %>% 
  filter(
    decree != 999, 
    on_off == 1
    ) %>%
  mutate(
    decree       = as.numeric(decree)
  ) %>%
  mutate(
    decree_af    = decree*days_in_month*af_per_cfs,
    decree_af    = as.numeric(decree_af),
    district     = as.character(district),
    priority     = as.numeric(priority),
    on_off       = as.integer(on_off)
  ) %>%
  group_by(node_id) %>%
  filter(if_all(cols_to_check, ~ !is.na(.x))) %>%                               
  ungroup() %>% 
  relocate(date,      year,      month,    days_in_month,   basin,           district, 
           node_type, node_id,   demand,   supply,          shortage,        sup_direct_flow, shortage_direct, 
           id,        name,      admin,    decree,          on_off,          decree_af,       priority) 



length(unique(statemod_ddr$node_id))
length(unique(ddr$node_id))
length(unique(statemod_ddr$district))

# Save output
saveRDS(statemod_ddr, "data/statemod/nodes/statemod_ddr.rds")
# readr::write_csv(statemod_ddr, "data/statemod/nodes/statemod_ddr.csv")

# remove extra dataframes
rm(statemod, indiv_decrees, ddr, af_per_cfs, cols_to_check)







