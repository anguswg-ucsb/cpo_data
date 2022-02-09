# Join statemod datasets with climate/reservoir data

# Output datasets:
  # 1. StateMod district + climate             - (month) 
  # 3. StateMod district + climate             - (year) 
  # 3. StateMod nodes + climate                - (month)
  # 4. StateMod nodes + climate  + reservoirs  - (month)
  # 5. StateMod shortages by right + climate   - (month)


library(tidyverse)

# **************************
# ---- Read in datasets ----
# **************************

# Climate data
climate_month       <- read_csv("data/climate/climate_month.csv")
climate_year        <- read_csv("data/climate/climate_year.csv")

# StateMod districts
statemod_month      <- read_csv("data/statemod/districts/statemod_district_month.csv") %>% 
                            mutate(district = as.numeric(district))

statemod_year       <- read_csv("data/statemod/districts/statemod_district_year.csv") %>% 
                            mutate(district = as.numeric(district))

# StateMod nodes
statemod_node       <- readRDS("data/statemod/nodes/statemod_node_data.rds") %>% 
                            mutate(district = as.numeric(district))

# StateMod shortages by right
shortages_by_right  <- readRDS("data/statemod/rights/shortages_by_right.rds")

# Reservoirs by node ID
reservoirs          <- readRDS("data/reservoirs/outputs/reservoir_nodes_summary.rds") %>% 
                            rename(node_id = dest_id)

# *******************

# check districts 
unique(statemod_year$district)
length(unique(statemod_year$district))

# check districts 
unique(statemod_month$district)
length(unique(statemod_month$district))

# check districts 
unique(statemod_node$district)
length(unique(statemod_node$district))

# check node ID 
length(unique(statemod_node$node_id))

# *******************
# ---- join data ----
# *******************

# ---- StateMod district + climate - (month) ----
statemod_district_climate_month  <- left_join(
                                      statemod_month,
                                      climate_month,
                                      by = c("date", "district")
                                    )
# save to /final
saveRDS(statemod_district_climate_month, "data/final/statemod_climate_month.rds")
# write.csv(statemod_district_climate_month, "data/final/statemod_climate_month.csv", row.names = F)

# ---- StateMod district + climate - (year) ----
statemod_district_climate_year   <- left_join(
                                      statemod_year,
                                      climate_year,
                                      by = c("year", "district")
                                    )
# save to /final
saveRDS(statemod_district_climate_year, "data/final/statemod_climate_year.rds")
# write.csv(statemod_district_climate_year, "data/final/statemod_climate_year.csv", row.names = F)

# ---- StateMod node + climate - (month) ----
statemod_node_climate <- left_join(
                                  statemod_node,
                                  climate_month,
                                  by = c("date", "district")
                                )

# save to /final
saveRDS(statemod_node_climate, "data/final/statemod_node_climate.rds")
# write.csv(statemod_node_climate, "data/final/statemod_node_climate.csv", row.names = F)


# ---- StateMod node + reservoir - (month) ----

# unique reservoir and statemod Node IDs
res_ids      <- unique(reservoirs$node_id)
stm_node_ids <- unique(statemod_node$node_id)

res_ids %in% stm_node_ids
stm_node_ids %in% res_ids

statemod_node_res_climate <- inner_join(
                              statemod_node,
                              reservoirs,
                              by = c("date", "node_id")
                              ) %>% 
                            left_join(
                              climate_month,
                              by = c("date", "district")
                              )


# save to /final
saveRDS(statemod_node_res_climate, "data/final/statemod_node_res_climate.rds")
# write.csv(statemod_node_res_climate, "data/final/statemod_node_res_climate.csv", row.names = F)





