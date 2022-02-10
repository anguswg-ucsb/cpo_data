# Angus Watters
# Direct Diversion Record file processing 
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


# DDR data folder path
ddr_path <- "data/ddr/raw/"

# list of DDR file names
ddr_files <- list.files(ddr_path, pattern = ".txt")

# empty list to use in For Loop
ddr_lst <- list()

# basin names for For Loop Logger
basin_names <- data.frame(
                          basins    =  c("colorado", "gunnison", "san_juan", "south_platte", "white", "yampa"), 
                          file_name =  c("cm", "gm", "sj", "sp", "wm", "ym")
                          )


for (i in 1:length(ddr_files)) {
  
  # log output text
  log_txt <- basin_names %>% 
    filter(file_name == substr(ddr_files[i], 1, 2) ) %>% 
    dplyr::select(basins)
  
  logger::log_info("tidying DDR file - {log_txt$basins}")
  
  # Column headers
  names <- c("id", "name", "struct", "admin", "decree", "on_off")
  
  
  # Direct Diversion Record 
  ddr  <- readr::read_fwf(
                         file           = paste0(ddr_path, ddr_files[i]),
                         col_types      = cols(),
                         col_positions  = fwf_widths(c(12, 24, 17, 12, 11, 8))
                         ) %>%
    setNames(names) %>% 
    slice(n = -1) %>% 
    mutate(
      admin     = as.character(admin)
    ) %>% 
    group_by(struct) %>% 
    mutate(
      on_off    = as.character((on_off))
    ) %>% 
    mutate(
      basin     = log_txt$basins
    ) %>% 
    group_by(struct) %>% 
    mutate(
      admin2      = as.numeric(admin),
      priority    = rank(admin2, ties.method = "first")
    ) %>% 
    dplyr::select(-admin2) %>% 
    dplyr::relocate(basin, name, id, struct, admin, decree, priority, on_off) %>%
    dplyr::rename("node_id" = "struct") %>% 
    ungroup()
  
  # replace any blank cells with NA 
  ddr[ddr == ""] <- NA
  
  # add to list of clean DDR dataframes
  ddr_lst[[i]] <- ddr
}

# bind rows
ddr_df <- bind_rows(ddr_lst)

length(unique(ddr_df$node_id))

# save output
saveRDS(ddr_df, "data/ddr/outputs/ddr.rds")
readr::write_csv(ddr_df, "data/ddr/outputs/ddr.csv")

rm(ddr_df, ddr_lst, ddr_path, log_txt, names, ddr, i, ddr_files, basin_names)





