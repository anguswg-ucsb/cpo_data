library(tidyverse)
library(readr)
library(stringr)
library(lubridate)
library(readxl)
library(progress)
library(sf)
library(logger)

# TSTool data folder path
tstool_path <- "data/tstool/output/"

# district shapefile path
shp_path = "data/spatial/water_districts.shp"

# basin shapefile
shp <- sf::st_read(shp_path, quiet = TRUE)  %>% 
  sf::st_transform(4326) %>%
  sf::st_cast("MULTIPOLYGON")

shp2 <- shp %>%
  st_drop_geometry()

# TSTool statemod data outputs 
out_files <- tibble(file_name = list.files(tstool_path)) %>% 
  mutate(
    district = substr(file_name, 26, 27)
  ) 


shp2 <- shp %>% 
  sf::st_drop_geometry()

# ---- For loop to clean up TSTool statemod data outputs ----

#   ### Wrangle statemod data - only select irrigation nodes and pivot longer
out_lst <- list()

rm(stm, irr_nodes2, irr_nodes, distr)
for (i in 1:length(out_files$file_name)) {
  
  logger::log_info("tidying StateMod data - district {out_files$district[i]}")
  
  # TSTool data folder path
  tstool_path <- "data/tstool/output/"
  
  # read in district statemod data, remove duplicate columns
  stm <- read_csv(
                  paste0(tstool_path,  out_files$file_name[i]),
                  col_types = cols()
                 ) %>% 
    dplyr::select(-ends_with("_1"))
  
  # select desired columns
  irr_nodes <- stm %>%
    dplyr::select(
      Date,
      contains("IrrigDemand"),   contains("IrrigSupply"), 
      contains("IrrigShortage"), contains("IrrigSupDirectFlow"),
      contains("MunDemand"),     contains("MunSupply"),
      contains("MunShortage"),   -contains("AGG"),
      -contains("ADP"),          -contains(".1")
    ) %>%
    mutate(
      Date           = paste0(Date,"-01"),
      Date           = as.Date(Date),
      year           = year(Date),
      month          = month(Date),
      days_in_month  = days_in_month(Date),
    )
  "0801004"
  # preserve column names
  new_names <- names(irr_nodes)
  
  # fix character numbers that have multiple decimals, remove all values after second decimal point
  irr_nodes <- irr_nodes %>% 
    lapply(
      function(x) gsub(
        "^([^.]*.[^.]*).*$", 
        "\\1", 
        x
      )
    ) %>%
    data.frame() %>% 
    setNames(new_names)
  
  # convert columns to numerics, pivot data long, add agr/mun indicator column
  irr_nodes <- irr_nodes %>% 
    mutate(
      across(c(-Date, -year, -month, -days_in_month), as.numeric)
    ) %>% 
    pivot_longer(
      cols = c(-Date, -year, -month, -days_in_month),
      names_to  = "node_id", 
      values_to = "af"
    ) %>% 
    mutate(
      node_type = case_when(
        grepl("_Irrig", node_id) ~ "agr",
        grepl("_Mun", node_id)   ~ "mun"
      )
    )
  
  irr_nodes <- irr_nodes %>% 
    mutate(
      node_id = case_when(
        grepl("_Irrig", irr_nodes$node_id) ~ as.data.frame(str_split_fixed(irr_nodes$node_id, "_Irrig", 2))[,1],
        grepl("_Mun", irr_nodes$node_id)   ~ as.data.frame(str_split_fixed(irr_nodes$node_id, "_Mun", 2))[,1]
      ),
      type    = case_when(
        grepl("_Irrig", irr_nodes$node_id) ~ as.data.frame(str_split_fixed(irr_nodes$node_id, "_Irrig", 2))[,2],
        grepl("_Mun", irr_nodes$node_id)   ~ as.data.frame(str_split_fixed(irr_nodes$node_id, "_Mun", 2))[,2]
        # grepl("_Mun", irr_nodes$node_id)   ~ paste0("Municipal ", as.data.frame(str_split_fixed(irr_nodes$node_id, "_Mun", 2))[,2])
      )
    ) %>% 
    setNames(c("Date", "year", "month", "days_in_month", "node_id",  "af", "node_type", "type"))
  
  # if else to account for nodes that don't have any agricultural nodes 
  if(any(grepl("SupDirectFlow", irr_nodes$type))) {
    
    # widen dataframe and calculate direct flow shortage
    irr_nodes2 <- irr_nodes %>%   
      pivot_wider(
        names_from = "type",
        values_from = "af"
      ) %>%
      mutate(Shortage_direct = Demand - SupDirectFlow) %>% 
      janitor::clean_names() %>% 
      mutate(                                                # replace negative direct flow shortage values w/ 0
        shortage_direct = case_when(
          shortage_direct < 0 ~ 0,
          TRUE                ~ shortage_direct
        )
      )
    
    # district column
    distr <- str_sub(irr_nodes2$node_id[1], 1, 2)
    
    # add district column and reorder columns
    irr_nodes2 <- irr_nodes2 %>% 
      mutate(district = distr) %>% 
      dplyr::relocate(date, year, month, days_in_month, district, node_id, node_type,
                      demand, supply, shortage, sup_direct_flow, shortage_direct)
  } else {
    # widen dataframe and place NA values in direct flow shortage & supply columns
    irr_nodes2 <- irr_nodes %>%   
      pivot_wider(
        names_from = "type",
        values_from = "af"
      ) %>%
      mutate(
        Shortage_direct = NA_real_,
        sup_direct_flow = NA_real_
      ) %>% 
      janitor::clean_names() 
    
    # district column
    distr <- str_sub(irr_nodes2$node_id[1], 1, 2)
    
    # add district column and reorder columns
    irr_nodes2 <- irr_nodes2 %>% 
      mutate(district = distr) %>% 
      dplyr::relocate(date, year, month, days_in_month, district, node_id, node_type,
                      demand, supply, shortage, sup_direct_flow, shortage_direct)
  }
  
  # add district/node data to list of district nodes
  out_lst[[i]] <- irr_nodes2
  rm(irr_nodes2, irr_nodes, new_names, shp_path, tstool_path, distr)
}

# bind rows
out_df <- bind_rows(out_lst)

# number of unique node_id in out_df
length(unique(out_df$node_id))

# save
saveRDS(out_df, "data/statemod/nodes/statemod_node_data.rds")
# write.csv(out_df, "data/outputs/statemod_node_data.csv", row.names = F)





















