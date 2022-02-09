# Apply a Multiple linear Regression model to annual district level StateMod data

# Number of districts = 50
# Each district has 32 usable years of data on record
# individual MLR models are built for each district (50 districts, 50 models)
# Dependent variable  = Annual Direct Flow Shortage as a percent of total Demand (short_dir_pct_dem)
# Modeling steps:
  # 1. Log transform short_dir_pct_dem 

  # 2. MLR model: short_dir_pct_dem = all climate variables

  # 3. Apply stepwise regression:
        # Stepwise regression: forward - Build regression model from set candidate predictor variables by
            # entering predictors based on p values, in a stepwise manner until  no variable left to enter

        # Stepwise regression: Both direction - Build regression model from set of candidate predictor variables by 
            # entering & removing predictors based on p values, in a stepwise manner until no variable left to enter or remove

  # 4. Variable Inflation Factor to remove predictors

  # 5.
remove(list = ls())
library(tidyverse)
library(olsrr)
library(car)
library(logger)
library(leaflet)
library(RColorBrewer)
library(viridis)

# data utils functions
source("utils/data_utils.R")

shp_path <- "data/spatial/water_districts.shp"

statemod_year <- read_csv("data/final/statemod_climate_year.csv") %>% 
  filter(year > 1980, year < 2013)
  # filter( year == 2013)

# stm_month <- stm %>%
#   filter(district == "72") %>%
#   mutate(variance = var(short_dir_norm))

statemod_district <- statemod_year %>%
  group_by(district) %>%
  group_split()

# tmp <- stm %>% filter(district == "9")

sensitivity_lst <- list()
vif_lst         <- list()

# statemod_district2 <- statemod_district[1:3]
# i = 2
for (i in 1:length(statemod_district)) {
  
  # annual district data
  df    <- bind_rows(statemod_district[[i]])
  
  # district text
  distr <- df$district[1]
  
  # calculate variance and standard deviation
  annual_short <- df %>%
    ungroup() %>%
    mutate(
      variance = var(short_dir_pct_dem),
      sd       = sd(short_dir_pct_dem)
    ) %>%
    dplyr::select(variance, sd)
  
  # variance
  year_variance <- annual_short$variance[1]
  
  # standard deviation
  year_sd       <- annual_short$sd[1]
  
  logger::log_info("Running MLR - district {distr}")
  
  # select columns for model
  df <- df %>%
    ungroup() %>%
    dplyr::select(short_dir_pct_dem, prcp:eddi12)
  
  # # subset and log transform data for MLR
  # log_trans <- df %>%
  #   ungroup() %>%
  #   mutate(
  #     short_dir_pct_dem = log10(short_dir_pct_dem)
  #   )
  
  if(sum(df$short_dir_pct_dem) <= 0) {
    
    log_info("District {distr} has no shortage data on record to use in model")
    
    # climate sensitivity dataframe
    sensitivity_df <- data.frame(
      district         = distr,
      var_sensitivity  =  NA_real_,
      sd_sensitivity   =  NA_real_,
      r2               =  NA_real_,
      variance         = year_variance,
      sd               = year_sd
    )
    
    # vif dataframe
    vf_df          <- data.frame(
      district = distr,
      variable = NA,
      vif      = NA_real_
    ) 
    
    
    vif_lst[[i]]         <- vf_df
    sensitivity_lst[[i]] <- sensitivity_df
    
  } else {
    
    # subset and log transform data for MLR
    log_trans <- df %>%
      ungroup() %>%
      mutate(
        short_dir_pct_dem = log10(short_dir_pct_dem)
      )
    
    # replace Infinite w/ 0
    is.na(log_trans) <- sapply(log_trans, is.infinite)
    log_trans[is.na(log_trans)] <- 0
    
    # MLR w/ VIF reduction + stepwise regression
    mlr_vfit <- lm(short_dir_pct_dem~., data = log_trans) %>%
      ols_step_both_p()
    # ols_step_forward_p()
    
    if(length(mlr_vfit$predictors) > 1) {
      
      # Calculate VIF
      vf <- car::vif(mlr_vfit$model)
      
      # VIF - remove variables w/ VIF > 5
      vf_df <- data.frame(vif = vf) %>%
        rownames_to_column() %>%
        arrange(vif) %>% 
        slice(n = 1:3)
        # filter(vif <= 5)
      
      # step wise regression
      lm_step <- lm(
        as.formula(paste("short_dir_pct_dem", paste(vf_df$rowname, collapse=" + "), sep=" ~ ")),
        data = log_trans)
      
      # R²
      r2 <-  summary(lm_step)$r.squared
      
      # climate sensitivity dataframe
      sensitivity_df <- data.frame(
        district         = distr,
        var_sensitivity  = r2*year_variance,
        sd_sensitivity   = r2*year_sd,
        r2               = r2,
        variance         = year_variance,
        sd               = year_sd
      )
      
      # vif dataframe
      vf_df          <- data.frame(vif = vf) %>%
        rownames_to_column() %>%
        arrange(vif) %>%
        mutate(district = distr) %>%
        setNames(c("variable", "vif", "district")) %>%
        dplyr::relocate(district)
      
      vif_lst[[i]]         <- vf_df
      sensitivity_lst[[i]] <- sensitivity_df
      
    } else {
      
      log_info("Stepwise regression yeilds single predictor variable - {mlr_vfit$predictors}")
      
      # R²
      r2 <- mlr_vfit$metrics$r2
      
      # climate sensitivity dataframe
      sensitivity_df <- data.frame(
        district         = distr,
        var_sensitivity  = r2*year_variance,
        sd_sensitivity   = r2*year_sd,
        r2               = r2,
        variance         = year_variance,
        sd               = year_sd
      )
      
      vf_df          <- data.frame(
        district = distr,
        variable = mlr_vfit$predictors, 
        vif      = NA_real_
      ) 
      vif_lst[[i]]         <- vf_df
      sensitivity_lst[[i]] <- sensitivity_df
    }
  }
  rm(r2, mlr_vfit, log_trans, lm_step, year_variance, year_sd, df, vf, vf_df)
}

# MLR results
mlr_df <- sensitivity_lst %>% 
  bind_rows()  %>% 
  mutate(
    var_sensitivity_log  = log10(var_sensitivity),
    sd_sensitivtiy_log   = log10(sd_sensitivity),
    var_sensitivity_norm = normalize(var_sensitivity),
    sd_sensitivity_norm  = normalize(sd_sensitivity)
    )

write.csv(mlr_df, "data/models/mlr_metrics.csv", row.names = F)

# variables after stepwise regresiion, before VIF removal
vif_df <- bind_rows(vif_lst)

# # Predictors selected after VIF 
predictors <- vif_df %>%
  group_by(district) %>%
  slice(1:3)

write.csv(predictors, "data/models/mlr_predictors.csv", row.names = F)

# ********************************************************
# ---- Plot climate sensitivity metrics on Leaflt map ----
# ********************************************************

# remove districts with NA values
mlr_map_data <- mlr_df %>%
  na.omit() 

# district shapefile path
shp_path <- "data/spatial/water_districts.shp"


# load shapefiles as spatial polygon object
shp <- sf::read_sf(paste0(shp_path), quiet = TRUE) %>%
  filter(DISTRICT %in% unique(mlr_map_data$district)) %>%
  st_transform(4326) %>%
  st_cast("MULTIPOLYGON") %>% 
  rename(district = DISTRICT, basin = BASIN) %>% 
  dplyr::select(district, basin, geometry)


# Join MLR results w/ district shapefile
mlr_shp <- left_join(
                    shp, 
                    mlr_map_data,
                    by = c("district")
                  )



pal <- colorNumeric("RdYlBu", domain = mlr_shp$var_sensitivty_norm, reverse = T, n = 30)
pal <- colorNumeric("RdYlBu", domain = mlr_shp$var_sensitivity_log, reverse = T, n = 30)
pal2 <- colorNumeric("RdYlBu", domain = mlr_shp$var_sensitivity, reverse = T, n = 30)

# pal2 <- colorNumeric("RdYlBu", domain = mlr_shp$sd_sensitivty_norm, n = 30)
# pal2 <- colorNumeric(c("darkred", "brown1", "yellow"), domain = centroids$af_total)

# Leaflet map
leaflet() %>%
  addProviderTiles(providers$OpenStreetMap, group = "Topographic") %>%
  addPolygons(
    data = mlr_shp,
    color = "black",
    fillOpacity = 0.8,
    fillColor = ~pal(var_sensitivity_log),
    # fillColor = ~pal(var_sensitivity),
    weight = 2,
    label = ~district,
    group = "Variance x R2",
    labelOptions = labelOptions(
      noHide = F,
      # direction = 'center',
      # textOnly = F)
      style = list(
        "color" = "black",
        "font-weight" = "1000")
    )
  ) %>% 
  # addLayersControl(overlayGroups = c(
  #   "SD x R2",
  #   "Variance x R2"
  # )) %>% 
  addLegend(
    data = mlr_shp,
    "bottomright",
    pal = pal,
    values = ~var_sensitivity_log,
    title = "Variance x R2",
    labFormat = labelFormat(digits = 10,),
    opacity = 1
  ) 





















