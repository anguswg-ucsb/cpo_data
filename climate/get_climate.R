# Angus Watters
# Script for pulling climate data and aggregating by district using climateR 

remove(list = ls())  # clear all workspace variables
library(tidyverse)
library(raster)
library(sp)
library(sf)
library(elevatr)
library(doParallel)
library(zoo)
library(tsbox)
library(SPEI)
library(lubridate)
library(climateR)
library(AOI)

# data utils functions
source("utils/data_utils.R")

shp_path = "data/spatial/water_districts.shp"

# load shapefiles as spatial polygon object
shp <- st_read(paste0(shp_path), quiet = T) %>%
  st_transform(4326) %>% 
  st_cast("MULTIPOLYGON") 

extra_distr <- c(71, 72, 73, 76, 77, 78, 79, 80)

distr <- shp %>% 
  st_drop_geometry() %>% 
  janitor::clean_names() %>% 
  arrange(district) %>% 
  dplyr::select(district, basin) %>% 
  filter(district %in% extra_distr)
  # filter(district %in% c("5"))

# ******************
# --- TerraClim ----
# ******************

rm(df, terra_lst, district_lst, terra, terra2)

terra_lst <- list()
district_lst <- list()

clim_vars <- c("prcp", "tmax", "tmin", "palmer", "aet", "pet",  "soilm", "swe")

# clim_vars <- c("prcp", "tmax", "tmin")
# i = 1
# k = 1
for (i in 1:nrow(distr)) {
  
  logger::log_info("Getting climate data - district {distr$district[i]}")
  
  terra_lst <- list()
  
  for (k in 1:length(clim_vars)) {
    
      logger::log_info("Processing {clim_vars[k]}  -  district {distr$district[i]}")
  
      terra <- get_terra(
                        district   = distr$district[i],
                        param      = clim_vars[k],
                        start_date = "1969-01-01",
                        end_date   = "2013-02-01",
                        # start_date = "1970-01-01",
                        # end_date   = "1970-02-01",
                        shp_path   =  shp_path
                      )

        if(clim_vars[k] == "prcp" | clim_vars[k] == "palmer" | clim_vars[k] == "soilm" | clim_vars[k] == "aet" | clim_vars[k] == "pet") {
          
          logger::log_info("masking areas above 2800m")
          
          # Summarize climate data and remove points above 2800 meters
          terra <- terra %>%
            add_elev(subdate = "1970-01-01") %>%
            filter(elevation <= 2800) %>%
            group_by(district, date) %>%
            summarise(value = mean(value)) %>%
            setNames(c("district", "date", clim_vars[k])) %>% 
            pivot_longer(
                      cols      = c(clim_vars[k]),
                      names_to  = "variable", 
                      values_to = "value"
                      ) %>% 
            ungroup()
          
          terra_lst[[k]] <- terra
          
        } else {
          # Summarize climate data and remove points above 2800 meters
          terra <- terra %>%
            group_by(district, date) %>%
            summarise(value = max(value)) %>%
            setNames(c("district", "date", clim_vars[k])) %>% 
            pivot_longer(
                      cols      = c(clim_vars[k]),
                      names_to  = "variable", 
                      values_to = "value"
                      ) %>% 
            ungroup()
          
          terra_lst[[k]] <- terra
        }
  }
  
  # bind rows of TerraClim data
  df <- bind_rows(terra_lst)
  
  logger::log_info("Calculating tavg")
  
  # average tmax and tmin
  tavg_df <- df %>%
    filter(variable %in% c("tmax", "tmin")) %>%
    pivot_wider(
      id_cols     = c(district:date),
      names_from  = "variable",
      values_from = "value"
    ) %>%
    group_by(date) %>% 
    mutate(tavg = (tmax + tmin)/2) %>% 
    ungroup() %>% 
    dplyr::select(district, date, tavg) %>% 
    pivot_longer(
      cols      = c(tavg),
      names_to  = "variable", 
      values_to = "value"
    ) 
  
  df <- bind_rows(df, tavg_df)
  
  # SPI timescales to calculate
  spi_timescales <- c(1, 3, 6, 9 ,12)
  
  # empty list for SPI data
  spi_lst <- list()
  
  for (z in 1:length(spi_timescales)) {
    
      logger::log_info("calculating SPI - {spi_timescales[z]} month")
      
      prcp_df <- df %>% 
        filter(variable == "prcp") %>% 
        pivot_wider(
          id_cols     = c(district:date), 
          names_from  = "variable", 
          values_from = "value"
        ) 
      
      spi <- get_spi(
          prcp        = prcp_df,
          start_year  = 1969,
          end_year    = 2012,
          timescale   = spi_timescales[z]
        )  %>% 
        pivot_longer(
          cols      = c(-date),
          names_to  = "variable", 
          values_to = "value"
        )
      spi_lst[[z]] <- spi
      
  }
  
  # Bind SPI data + add district column
  spi_df <- bind_rows(spi_lst) %>% 
    mutate(district = as.character(distr$district[i]))
  
  # bind TerraClim and SPI data
  full_df <- bind_rows(df, spi_df)
  
  # add completed climate dataset to list of district
  district_lst[[i]] <- full_df
  
  # rm(df, full_df, spi, spi_df, spi_lst, terra_lst, prcp_df, terra, eddi_lst, eddi_df, eddi)
  
}

# bind rows and widen to 
districts_terra <- bind_rows(district_lst) %>% 
  pivot_wider(
    id_cols     = c(district:date), 
    names_from  = "variable", 
    values_from = "value"
  )  %>% 
  rename(pdsi = palmer)

# save
# write.csv(districts_terra, "data/climate/terraclim_month.csv", row.names = F)

# *************
# --- EDDI ----
# *************

time = seq(
  ymd("1980-01-01"),
  ymd("2013-02-01"),
  by = '1 month'
)

# EDDI timescales to calculate
# eddi_timescales <- c(1, 3, 6, 9 ,12)
  
rm(eddi_lst, eddi_df, district_lst2, eddi, df, i)

# empty list for SPI data
eddi_lst <- list()

# rm(eddi_timescales)
for (i in 1:length(time)) {
          # logger::log_info("downloading EDDI - {eddi_timescales[j]} month")
    logger::log_info("downloading EDDI - {time[i]} - 12 month")

          eddi <- get_eddi(
                district    = distr$district,
                start_date  = time[i],
                # start_date  = "1980-01-01",
                # end_date    = "1990-01-01", 
                shp_path    = shp_path, 
                timestep    = 12
                # timestep    = eddi_timescales[j]
              )
          
          eddi <- eddi %>% 
              group_by(district, date) %>%
              summarise(value = mean(eddi)) %>%
              setNames(c("district", "date", paste0("eddi", 12))) %>%
              pivot_longer(
                cols      = c(paste0("eddi",12)),
                names_to  = "variable",
                values_to = "value"
              )  %>%
            ungroup()
          
          eddi_lst[[i]] <- eddi
}


eddi_df <- bind_rows(eddi_lst)

# save EDDI data
write.csv(eddi_df, "data/climate/eddi12.csv", row.names = F)

# clear workspace
# remove(list = ls())

eddi_files    <- paste0("data/climate/", list.files("data/climate/", pattern = "eddi"))[2:6]
# eddi_files    <- paste0("data/climate/", list.files("data/climate/", pattern = "eddi"))

timescale_lst <- list()

# bind all EDDI timescales to one dataframe
for(i in 1:length(eddi_files)){
  logger::log_info("reading {eddi_files[i]}")

  eddi   <- read_csv(eddi_files[i])

  timescale_lst[[i]] <- eddi
}
timescale_lst[[1]]

# widen columns to each EDDI timescale
eddi_wide <- timescale_lst %>% 
  bind_rows() %>%
  pivot_wider(
    id_cols     = c(district:date),
    names_from  = "variable",
    values_from = "value"
  ) %>% 
  dplyr::relocate(district, date, eddi1, eddi3, eddi6, eddi9, eddi12)

# save wide EDDI data
write.csv(eddi_wide, "data/climate/eddi.csv", row.names = F)

# *******************************
# ---- Join TerraClim + EDDI ----
# *******************************

# TerraClim
terraclim <- read_csv("data/climate/terraclim_month.csv")

# rm(eddi_df, eddi_lst, eddi_wide, eddi, timescale_lst, i, eddi_files)

# EDDI 1, 3, 6, 9, 12
eddi     <- read_csv("data/climate/eddi.csv")

climate <- full_join(
                    terraclim, 
                    eddi, 
                    by = c("date", "district")
                  ) %>% 
  dplyr::relocate(district, date, prcp, tavg, tmax, tmin)

# save all climate data
# write.csv(climate, "data/climate/climate_month.csv", row.names = F)

# yearly district level climate data
clim_year <- climate %>%
  mutate(
    year = lfstat::water_year(date)
  ) %>%
  group_by(district, year) %>%
  summarize(
    prcp       = mean(prcp, na.rm = T),
    tavg       = mean(tavg, na.rm = T),
    tmax       = mean(tmax, na.rm = T),
    tmin       = mean(tmin, na.rm = T),
    pdsi       = mean(pdsi, na.rm = T),
    aet        = mean(aet, na.rm = T),
    pet        = mean(pet, na.rm = T),
    soilm      = mean(soilm, na.rm = T),
    swe        = sum(swe, na.rm = T),
    # swe_max     = max(swe),
    # swe_max_nrcs = max(swe_max),
    # swe_cuml   = mean(swe_cuml),
    spi1       = mean(spi1, na.rm = T),
    spi3       = mean(spi3, na.rm = T),
    spi6       = mean(spi6, na.rm = T),
    spi9       = mean(spi9, na.rm = T),
    spi12      = mean(spi12, na.rm = T),
    eddi1      = mean(eddi1, na.rm = T),
    eddi3      = mean(eddi3, na.rm = T),
    eddi6      = mean(eddi6, na.rm = T),
    eddi9      = mean(eddi9, na.rm = T),
    eddi12     = mean(eddi12, na.rm = T)
  ) %>% 
  ungroup()

# unique(clim_year$district)

# save all yearly climate data
write.csv(clim_year, "data/climate/climate_year.csv", row.names = F)

# clear workspace
remove(list = ls())

# test plot SWE
# swe_df <- climate %>% 
#   mutate(
#     wyear = as.numeric(as.character(lfstat::water_year(date)))
#          ) %>% 
#   # filter(district %in% c(6, 7, 1, 64)) %>%
#   group_by(wyear, district) %>% 
#   summarize(swe_sum = sum(swe))
# ggplot() +
#   geom_col(data = swe_df, aes(x = wyear, y = swe_sum)) +
#   gghighlight::gghighlight(wyear == 2002) +
#   facet_wrap(~district)


















