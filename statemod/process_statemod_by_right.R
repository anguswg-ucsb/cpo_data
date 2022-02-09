# Rachel Bash & Angus Watters
# Process StateMod DDR data by right to calculate total and direct flow shortages by right
# 01/27/2022

# THIS LOOP WILL LOOP THROUGH THE StateMod DDR DATAFRAME CONTAINING THE STATEMOD OUTPUTS and WILL CACLUCALTE THE SHORTAGES BY RIGHT FOR EVERY NODE AND EVERY DATE OF THAT NODE, THE DATA IS THEN SAVED TO A SPECIFIED PATH (use path variable below)

remove(list = ls())
library(tidyverse)
library(readr)
library(lubridate)
library(logger)

# **********************************
# ---- Total shortages by ditch ----
# **********************************

# Read in Statemod DDR file
statemod           <- readRDS("data/statemod/nodes/statemod_ddr.rds")
# statemod <- readr::read_csv("data/statemod/nodes/statemod_ddr.csv") 

length(unique(statemod$node_id))

# Resolved ---- some nodes I had to rerun at one point
# missing <- c("0100518", "0100519_D","0200826", "0200822","0700647", "0801007", 
#             "0801009_D" ,"2302917","2302908", "2302907","2302900", "2302906","4100534_D","6400504")

statemod <- statemod %>% 
  # filter(node_id %in% missing) %>% 
  group_by(node_id, date) %>% 
  mutate(
    admin2    = as.numeric(admin),
    priority  = rank(admin2, ties.method = "first")
    ) %>% 
  dplyr::select(-admin2) %>% 
  ungroup()


# remove for loop outputs at start
rm(pb, lst, lst2, df, tmp, dates, demand, decree, supply_ditch, use, nodes, p, short, shortage_ditch, demand_ditch, supply_ditch, districts, final)

# unique nodes for for loop
all_nodes <- unique(statemod$node_id)

# unique districts for for loop
districts <- unique(statemod$district)

short_lst <- list()

for (k in 1:length(districts)) {
  # pb$tick()
  logger::log_info("District: {districts[k]}")
  
  df <- statemod %>%
    filter(district == districts[k], date >= "1970-10-01")
  # %>%  na.omit()
  
  nodes <- unique(df$node_id)
  dates <- unique(df$date)
  
  lst <- list()
  lst2 <- list()

  for (i in 1:length(nodes)) {
    
    node_txt <- paste0(" - (", i, "/", length(nodes), ")")
    logger::log_info("Node: {paste0(nodes[i], node_txt)}")
    
    tryCatch({
      for (d in 1:length(dates)) {
        
        # logger::log_info("Date: {dates[d]}")
        
        # isolate data from date d on node i
        tmp <- statemod %>%
          filter(date == dates[d], node_id == nodes[i]) %>%
          arrange(priority)
        
        tmp$supply2     <- NA
        tmp$demand2     <- NA
        tmp$short2      <- NA
        
        supply_ditch    = tmp$supply[1]
        demand_ditch    = tmp$demand[1]
        shortage_ditch  = tmp$shortage[1]
        
        for (p in tmp$priority) {
          
          # water right decree, monthly (af)
          decree = tmp$decree_af[p]
          
          # senior water right
          if (p == 1) {
            
            # right demand
            if (decree > demand_ditch) {
              
              demand = demand_ditch
              
            } else {
              
              demand = decree
              
            }
            
            if (decree > supply_ditch) {
              
              use = supply_ditch
              
              if (shortage_ditch > 0) {
                
                short = min(
                  demand_ditch - supply_ditch, decree, max(decree - use, 0)
                )
                
              } else if (supply_ditch > demand_ditch) {
                
                short = 0
                
              }
              else {
                
                short = 0
                
              }
            }
            
            else {
              
              use = decree
              short = 0
              
            }
          }
          else { # junior water rights
            # cumulative decree of right p and all other more senior rights
            # cum_decree = rollsumr(tmp$decree_af, k = 3)[1]
            
            # cumulative decree of all rights older than right p
            # cum_decree_wo = rollsumr(tmp$decree_af, k = 3-1)[1]
            
            cum_decree <- tmp %>%
              filter(priority <= p)
            cum_decree <- max(cumsum(cum_decree$decree_af))
            
            # # # cumalative decree of all rights older than right p
            cum_decree_wo <- tmp %>%
              filter(priority <= p-1)
            cum_decree_wo <- max(cumsum(cum_decree_wo$decree_af))
            
            # right demand
            if (cum_decree < demand_ditch) {
              
              demand = decree
              
            } else if (cum_decree_wo > demand_ditch) {
              
              demand = 0
              
            } else {
              
              demand = demand_ditch - cum_decree_wo
              
            }
            
            if (cum_decree < supply_ditch) {
              
              use = decree
              short = 0
              
            }
            
            else if (cum_decree_wo >= supply_ditch) {
              
              use = 0
              
              if (supply_ditch >= demand_ditch) {
                
                short = 0
                
              }
              
              else {
                
                if (cum_decree <= demand_ditch) {
                  
                  short = decree
                  
                }
                
                else {
                  
                  short = max(demand_ditch - cum_decree_wo, 0)
                  
                }
              }
            }
            
            else {
              
              use = supply_ditch - cum_decree_wo
              
              if (supply_ditch >= demand_ditch) {
                
                short = 0
                
              }
              else {
                
                short <-  min(demand_ditch - supply_ditch,
                              decree - use)
                
              }
            }
          }
          
          tmp$supply2[p] <- use
          tmp$demand2[p] <- demand
          tmp$short2[p] <- short
        }
        lst[[d]] <- tmp
        
      }
      lst2[[nodes[i]]] <- lst
      
    }, error=function(e){})
    
    final = bind_rows(lst2) %>%  
      dplyr::select(-demand, -supply, -shortage, -shortage_direct) %>% 
      dplyr::rename(
                    demand      = demand2,
                    supply      = supply2, 
                    supply_dir  = sup_direct_flow,
                    short       = short2
                    ) %>% 
      mutate(
                    demand      = round(demand, 4), 
                    supply      = round(supply, 4),
                    supply_dir  = round(supply_dir, 4),
                    short       = round(short, 4)
                    ) %>% 
      dplyr::relocate(
                    date,      year,    month,    days_in_month, basin,  district,
                    node_type, node_id, id,       name,          admin,  decree, 
                    decree_af, on_off,  priority, demand,        supply, supply_dir, short
                    )
  }
  # saveRDS(final, paste0("data/statemod/short_by_right_district_", districts[k], ".rds"))
  short_lst[[k]] <- final
}

# bind rows to single dataframe
short_df <- bind_rows(short_lst) 

# length(unique(short_df$node_id))

# save
saveRDS(short_df, "data/statemod/rights/short_by_right_v2.rds")
# readr::write_csv(short_df, "data/statemod/rights/short_by_right.csv")


# ****************************************
# ---- Direct flow shortages by ditch ----
# ****************************************

# Read in Statemod DDR file
statemod <- readRDS("data/statemod/nodes/statemod_ddr.rds")
# statemod <- readr::read_csv("data/statemod/statemod_ddr.csv") 


# Mundir)icipal nodes
# muni_nodes <-   c("04_AMP001_I" ,"04_AMP001_O", "04_ASI_EL", "04_ASI_TH" ,"05_AMP001_I" ,"05_AMP001_O", "05_ASI_EL", "05_ASI_TH", "06_ELDORA" ,"06_ASI_EL", "06_ASI_TH" ,"07_AMP001_I", "07_AMP001_O", "08_Aurora_I", "08_Aurora_O" ,"08_Denver_I", "08_Denver_O" ,"09_ASI_TH", "09_AMP001_I" ,"09_AMP001_O", "23_AMP001_I","23_AMP001_O", "30_SUIT" ,"3803713M1", "3803713M2", "4300564" ,"4306045" ,"4302571", "5700512" ,"5801869" ,"5801919", "5805059_D" ,"5805066" ,"5801583" ,"5805055", "6000511", "62_AMG001", "62U_MY" , "62L_MY" , "7201523", "7200816" ,"72_GJMunExp","7800562" ,"7804672" ,"80_AMP001_I" ,"80_AMP001_O", "9900528" )

statemod <- statemod %>%
  # filter(node_id %in% missing) %>%
  group_by(node_id, date) %>%
  mutate(
    admin2    = as.numeric(admin),
    priority  = rank(admin2, ties.method = "first")
  ) %>%
  dplyr::select(-admin2) %>%
  ungroup()

# remove for loop outputs at start
# rm(pb, lst, lst2, df, tmp, dates, demand, decree, supply_ditch, use, nodes, p, short, shortage_ditch, demand_ditch, supply_ditch, districts, final)
# rm(short_dir_lst, statemod, shortages_by_right, all_nodes, i, k, d, cum_decree, cum_decree_wo, districts, node_txt, lst, lst2, df, dates,nodes)

# unique nodes for for loop
all_nodes <- unique(statemod$node_id)

# unique districts for for loop
districts <- unique(statemod$district)

short_dir_lst <- list()

for (k in 1:length(districts)) {
  # pb$tick()
  logger::log_info("District: {districts[k]}")

  df <- statemod %>%
    filter(district == districts[k], date >= "1970-10-01")
  # %>% na.omit()
  
  nodes <- unique(df$node_id)
  dates <- unique(df$date)
  
  lst <- list()
  lst2 <- list()

  for (i in 1:length(nodes)) {
    
    node_txt <- paste0(" - (", i, "/", length(nodes), ")")
    logger::log_info("Node: {paste0(nodes[i], node_txt)}")
    
    tryCatch({
      for (d in 1:length(dates)) {
        
        # isolate data from date d on node i
        tmp <- statemod %>%
          filter(date == dates[d], node_id == nodes[i]) %>%
          arrange(priority)
        
        tmp$supply_dir    <- NA
        tmp$demand_dir    <- NA
        tmp$short_dir     <- NA
        
        supply_ditch      <-  tmp$sup_direct_flow[1]
        demand_ditch      <-  tmp$demand[1]
        shortage_ditch    <-  tmp$shortage_direct[1]
        
        for (p in tmp$priority) {
          
          # water right decree, monthly (af)
          decree = tmp$decree_af[p]
          
          # senior water right
          if (p == 1) {
            
            # right demand
            if (decree > demand_ditch) {
              
              demand = demand_ditch
              
            } else {
              
              demand = decree
              
            }
            
            if (decree > supply_ditch) {
              
              use = supply_ditch
              
              if (shortage_ditch > 0) {
                
                short = min(
                  demand_ditch - supply_ditch, decree, max(decree - use, 0)
                )
                
              } else if (supply_ditch > demand_ditch) {
                
                short = 0
                
              }
              else {
                
                short = 0
                
              }
            }
            
            else {
              
              use = decree
              short = 0
              
            }
          }
          else { # junior water rights
            # cumulative decree of right p and all other more senior rights
            # cum_decree = rollsumr(tmp$decree_af, k = 2)[1]
            
            # cumulative decree of all rights older than right p
            # cum_decree_wo = rollsumr(tmp$decree_af, k = 3-1)[1]
            
            cum_decree <- tmp %>%
              filter(priority <= p)
            cum_decree <- max(cumsum(cum_decree$decree_af))
            
            # # # cumalative decree of all rights older than right p
            cum_decree_wo <- tmp %>%
              filter(priority <= p-1)
            cum_decree_wo <- max(cumsum(cum_decree_wo$decree_af))
            
            # right demand
            if (cum_decree < demand_ditch) {
              
              demand = decree
              
            } else if (cum_decree_wo > demand_ditch) {
              
              demand = 0
              
            } else {
              
              demand = demand_ditch - cum_decree_wo
              
            }
            
            if (cum_decree < supply_ditch) {
              
              use = decree
              short = 0
              
            }
            
            else if (cum_decree_wo >= supply_ditch) {
              
              use = 0
              
              if (supply_ditch >= demand_ditch) {
                
                short = 0
                
              }
              
              else {
                
                if (cum_decree <= demand_ditch) {
                  
                  short = decree
                  
                }
                
                else {
                  
                  short = max(demand_ditch - cum_decree_wo, 0)
                  
                }
              }
            }
            
            else {
              
              use = supply_ditch - cum_decree_wo
              
              if (supply_ditch >= demand_ditch) {
                
                short = 0
                
              }
              else {
                
                short <-  min(demand_ditch - supply_ditch,
                              decree - use)
                
              }
            }
          }
          
          tmp$supply_dir[p] <- use
          tmp$demand_dir[p] <- demand
          tmp$short_dir[p] <- short
        }
        lst[[d]] <- tmp
        
      }
      lst2[[nodes[i]]] <- lst
      # setTxtProgressBar(pb,i)
      
    }, error=function(e){})
    final = bind_rows(lst2) %>%  
      dplyr::select(-demand, -supply, -shortage, -shortage_direct, -sup_direct_flow) %>% 
      dplyr::rename(
        demand_dir      = demand_dir,
        supply_dir      = supply_dir, 
        short_dir       = short_dir
      ) %>% 
      mutate(
        demand_dir      = round(demand_dir, 4), 
        supply_dir      = round(supply_dir, 4),
        short_dir       = round(short_dir, 4)
      ) %>% 
      dplyr::relocate(
        date,      year,    month,    days_in_month, basin,  district,
        node_type, node_id, id,       name,          admin,  decree, 
        decree_af, on_off,  priority, demand_dir,    supply_dir, short_dir
      )
  }
  
  short_dir_lst[[k]] <- final
  # saveRDS(final, paste0(path, "/short_dir_by_right_district_", districts[k], ".rds"))
  # saveRDS(final, paste0(path, "/ditch_shortage_dir/short_dir_by_right_district_", districts[k], ".rds"))
}

short_dir_df    <- bind_rows(short_dir_lst) 

# save output
saveRDS(short_dir_df, "data/statemod/rights/short_dir_by_right_v2.rds") 

# ************************************************
# ---- join shortages + direct shortages data ----
# ************************************************

shortages <- left_join(
                      short_df,
                      dplyr::select(short_dir_df, date, id, short_dir),
                      by = c("date", "id")
                    )

length(unique(statemod$district))
length(unique(short_dir_df$district))
length(unique(short_df$node_id))
length(unique(shortages$node_id))

# save output
saveRDS(shortages, "data/statemod/rights/shortages_by_right.rds")
# readr::write_csv(shortages, "data/statemod/rights/shortages_by_right.csv")

rm(short, tmp, tmp2, statemod, short_dir2, final, lst, lst2, df)







