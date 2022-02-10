# Angus Watters
# Script for cleaning reservoir files and joining with clean OPR files

library(tidyverse)
library(here)
library(stringr)

# ************************
# ---- Reservoir list ----
# ************************

# path to reservoir CSVs
reservoir_files <- list.files("data/reservoirs/reservoir_list/") 

# empty list to add reservoir data frames into
reservoir_lst <- list()

# iterate through each reservoir file and add into list to combine into single data frame 
for (i in 1:length(reservoir_files)) {
  
  logger::log_info("reservoir data: {reservoir_files[i]}")

  res_df              <- read_csv(paste0("data/reservoirs/reservoir_list/", reservoir_files[i])) %>% 
                                    janitor::clean_names() %>% 
                                    rename(reservoir_id = id)
  
  reservoir_lst[[i]]  <- res_df  
}

# bind data frames into single data frame 
reservoirs <- bind_rows(reservoir_lst)

# remove extra data
rm(res_df, reservoir_lst, reservoir_files, i)

# *******************
# ---- OPR files ----
# *******************

# Read in tidy OPR file dataframe w/ corresponding reservoir IDs
opr <- read_csv("data/opr/opr.csv")

# filter for reservoir IDs in the list of CO reservoirs dataframe
opr_res_id <- opr %>% 
  filter(reservoir_id %in% unique(reservoirs$reservoir_id))

# opr_dest_id <- opr %>% 
#   filter(dest_id %in% unique(reservoirs$reservoir_id)) %>% 
#   filter(!dest_id %in% unique(opr_res_id$dest_id))
  # mutate(reservoir_id = dest_id)

opr_all <- bind_rows(opr_res_id)
unique(opr_res_id$reservoir_id)
unique(opr_res_id$reservoir_id) %in% unique(opr_dest_id$reservoir_id)
unique(opr_dest_id$reservoir_id) %in% unique(opr_res_id$reservoir_id)

# remove extra data
rm(opr_dest_id, opr_res_id, opr)

# **************************
# ---- OPR + Reservoirs ----
# **************************

# join data by shared reservoir ID column
reservoir_opr <- inner_join(opr_all, reservoirs, by = "reservoir_id") 
length(unique(reservoir_opr$name_description))
length(unique(reservoir_opr$reservoir_id))
# reservoir_opr  <- inner_join(opr_res_id, reservoirs, by = "reservoir_id")

# save to data/reservoirs/outputs
# write.csv(reservoir_opr, "data/reservoirs/outputs/reservoir_opr.csv"))
# saveRDS(reservoir_opr, "data/reservoirs/outputs/reservoir_opr.csv"))

# ******************************
# ---- Reservoir timeseries ----
# ******************************
# path to reservoir CSVs
reservoir_ts_files <- list.files("data/reservoirs/reservoir_timeseries/") 

# empty list to add reservoir data frames into
reservoir_ts_lst <- list()

# iterate through each reservoir file and add into list to combine into single data frame 
for (i in 1:length(reservoir_ts_files)) {
  
  logger::log_info("reservoir ts data: {reservoir_ts_files[i]}")
  
  res_ts              <- read_csv(
                                paste0("data/reservoirs/reservoir_timeseries/", reservoir_ts_files[i]), 
                                col_types = cols()
                                ) %>% 
                            janitor::clean_names()
  
  remove <- c(
            "initial_storage", "total_supply", "total_release", "evap", "seep_spill", 
            "sim_eom", "river_inflow", "river_well", "river_outflow"
            )
  
  res_ts <- res_ts %>% 
    pivot_longer(!date, names_to = "var", values_to = "val") %>% 
    mutate(
      var2     = case_when(
                      grepl("initial_storage", var)    ~ "initial_storage",
                      grepl("total_supply", var)       ~ "total_supply",
                      grepl("total_release", var)      ~ "total_release",
                      grepl("evap", var)               ~ "evap",
                      grepl("seep_spill", var)         ~ "seep_spill",
                      grepl("sim_eom", var)            ~ "sim_eom",
                      grepl("river_inflow", var)       ~ "river_inflow",
                      grepl("river_well", var)         ~ "river_well",
                      grepl("river_outflow", var)      ~ "river_outflow"
                      ),
      txt      = str_remove_all(
                            substr(var, 2, nchar(var)), paste(remove, collapse = "|")
                      ),
      id       = substr(txt, 1, nchar(txt)- 1)
      )  %>% 
    dplyr::select(date, "variable" = var2, id, val)
    # id   =  str_extract(var, "\\_*\\d+\\_*\\d*")) %>%  dplyr::select(date, "variable" = var2, id, val)
  
    # res_ts$id <- sub("_$", "", res_ts$id)
  
    res_ts <- res_ts %>% 
        mutate(
                date = as.Date(paste0(date, "-01")),
                id = gsub("_", "-", id)
              ) %>% 
        rename(reservoir_id = id) %>% 
        pivot_wider(id_cols = -variable, names_from ="variable", values_from = "val") 
      
      reservoir_ts_lst[[i]] <- res_ts
  
}

# bind rows from list of reservoir data frames
reservoir_ts <- bind_rows(reservoir_ts_lst)


reservoir_ts <- reservoir_ts %>% 
  filter(reservoir_id %in% unique(reservoir_opr$reservoir_id))


# remove extra data
rm(res_ts, reservoir_files, reservoir_ts_files, remove, i, reservoir_ts_lst)


length(unique(reservoir_ts$reservoir_id))

# Join Reservoir timeseries data w/ Reservoir-OPR dataframe
reservoir_users <- left_join(
                            reservoir_ts, 
                            dplyr::select(reservoir_opr, id, dest_id, name, admin, reservoir_id),
                            by = "reservoir_id"
                          ) %>% 
                      as_tibble() %>% 
                      na.omit()

# compare # of unique reservoir_id in reservoir_user vs. reservoir_opr
length(unique(reservoir_users$reservoir_id))
length(unique(reservoir_opr$reservoir_id))

# check for duplicates
unique_res <- reservoir_users[!duplicated(reservoir_users[]),]

# remove extra data
rm(reservoir_ts, reservoir_ts_lst, reservoirs, opr_all, unique_res)

# save to data/reservoirs/outputs
saveRDS(reservoir_users, "data/reservoirs/outputs/reservoir_users.rds")
# write.csv(reservoir_users, "data/reservoirs/outputs/reservoir_users.csv", row.names = F)

# **********************
# ---- summary data ----
# **********************

# Percent of storage capacity across all accounts.
# If user has 3 accounts, then calculate:
       # initial_storage_pct = (initial_storage1 + initial_storage2 + initial_storage3) / (total_capacity1 + total_capacity2 + total_capacity3) * 100

# summarize data by dest_id and date
reservoir_summary <- reservoir_users %>% 
  group_by(reservoir_id) %>% 
  mutate(
    max_capacity = max(sim_eom)
  ) %>% 
  ungroup() %>% 
  # filter(dest_id == "7202003") %>%
  group_by(date, dest_id) %>%
  summarize(
    initial_storage      = sum(initial_storage),
    total_capacity       = sum(max_capacity),
    initial_storage_pct  = round((initial_storage/total_capacity)*100, 3),
    total_supply         = sum(total_supply),
    total_release        = sum(total_release),
    evap                 = sum(evap),
    seep_spill           = sum(seep_spill),
    sim_eom              = sum(sim_eom),
    river_inflow         = sum(river_inflow),
    river_well           = sum(river_well),
    river_outflow        = sum(river_outflow)
  ) %>% 
  ungroup()

# compare # of unique dest_id in reservoir_user vs. reservoir_summary
length(unique(reservoir_summary$dest_id))
length(unique(reservoir_users$dest_id))

# save to data/reservoirs/outputs
saveRDS(reservoir_summary, "data/reservoirs/outputs/reservoir_nodes_summary.rds")
# write.csv(reservoir_summary, "data/reservoirs/outputs/reservoir_nodes_summary.csv", row.names = F)

# ************************************************





















