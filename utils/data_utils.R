
# loads climateR gridmet data for desired paramter rasters, masks to the specified basin, seperates data by district, and aggregates to a tidy tibble
get_gridmet <- function(basin, param, start_date, end_date = NULL,
                     shp_path) {
  # read shapefile, filter to a basin, cast to a multipolygon for masking later on
  shp <- sf::st_read(shp_path, quiet = TRUE) %>%
    # dplyr::filter(DISTRICT %in% !!distr) %>%
    dplyr::filter(BASIN %in% !!basin) %>%
    sf::st_transform(4326) %>%
    sf::st_cast("MULTIPOLYGON") 
    # dplyr::filter(DISTRICT %in% c(64,  1,  4,  2,  5,  6,  7,  8,  9, 80, 23))
  
  # pull gridmet data from climateR gridMET
  gridmet <-
    climateR::getGridMET(
      shp,
      param     = param,
      startDate = start_date,
      endDate   = end_date
    ) %>%
    raster::stack()
  
  # mask stacks to districts 
  gridmet <- lapply(
    X   = seq_len(nrow(shp)),
    FUN = function(x) {
      raster::mask(gridmet, shp[x, ])
    }
  )
  stack_list     <- lapply(X = gridmet, FUN = raster::stack)       # stack list of masked rasterstacks 
  district_names <- paste0(shp$DISTRICT)                        # district number
  stack_list     <- setNames(stack_list, nm = district_names)   # add district names to stacks
  
  # create tidy tibbles from each raster stack in list, then wrangle date columns
  tidy_gridmet <- lapply(X = stack_list, FUN = tidy_raster) 
  tidy_gridmet <- lapply(
    X = names(tidy_gridmet),
    FUN = function(x) {
      dplyr::mutate(
        tidy_gridmet[[x]],
        district = x
      )
    }
  ) %>%
    bind_rows()
}


get_terra <- function(district, param, start_date, end_date = NULL,
                      shp_path) {
  
  shp <- sf::st_read(shp_path, quiet = TRUE) %>%
    dplyr::filter(DISTRICT %in% !!district) %>%
    sf::st_transform(4326) %>%
    sf::st_cast("MULTIPOLYGON")
  
  doParallel::stopImplicitCluster()
  
  terra <- climateR::getTerraClim(
    AOI       = sf::st_transform(shp, 4326),
    param     = param,
    startDate = start_date,
    endDate   = end_date
  ) %>% 
    stack()
  
  
  terra <- lapply(
    X   = seq_len(nrow(shp)),
    FUN = function(x) {
      raster::mask(terra, shp[x, ])
    }
  )
  
  
  stack_list     <- lapply(X = terra, FUN = raster::stack)       # stack list of masked rasterstacks 
  district_names <- paste0(shp$DISTRICT)                        # district number 
  stack_list     <- setNames(stack_list, nm = district_names)   # add district names to stacks
  
  # create tidy tibbles from each raster stack in list, then wrangle date columns
  tidy_terra <- lapply(X = stack_list, FUN = tidy_terra_raster)
  
  tidy_terra <- lapply(
    X = names(tidy_terra),
    FUN = function(x) {
      dplyr::mutate(
        tidy_terra[[x]],
        district = x
      )
    }
  ) %>%
    bind_rows()
}

get_spi <- function(prcp, start_year, end_year,  timescale = 1) {
  
  prcp_ts <- prcp %>%
    mutate(
      YEAR      = as.integer(lubridate::year(date)),
      MONTH     = as.integer(lubridate::month(date)),
      PRCP      = prcp
    ) %>%
    ungroup() %>%
    dplyr::select(YEAR, MONTH, PRCP)
  
  # add a date column from YEAR and MONTH cols
  prcp_ts$date <- zoo::as.yearmon(paste(prcp_ts$YEAR, prcp_ts$MONTH), "%Y %m")
  
  # make YEAR & MONTH columns integers, required for SPI function
  prcp_ts$YEAR <- as.integer(prcp_ts$YEAR)
  prcp_ts$MONTH <- as.integer(prcp_ts$MONTH)
  
  # SPI dataframe a timeseries
  prcp_ts <- ts(
    prcp_ts,
    start      = c(start_year, 1),
    end        = c(end_year, 12),
    frequency  = 12
  )
  
  # SPI column name
  spi_name <- paste0("spi", timescale)
  
  # calculate SPI 
  spi <- SPEI::spi(prcp_ts[,3], timescale)
  spi <- tsbox::ts_data.frame(spi$fitted) %>% 
    setNames(c("date", spi_name))
  
  return(spi)
  
  rm(prcp_ts, spi_name)
  
}

get_eddi <- function(district, start_date, end_date = NULL, shp_path, timestep = 1) {
  if(!is.null(end_date)) {
    # dates for EDDI 
    time = seq(
      ymd(start_date),
      ymd(end_date),
      by = '1 month'
      ) 
  } else {  
    time = seq(
      ymd(start_date),
      ymd(start_date),
      by = '1 month'
      )
  } 

  # read shapefile, filter to a basin, cast to a multipolygon for masking later on
  shp <- sf::st_read(shp_path, quiet = TRUE) %>%
    dplyr::filter(DISTRICT %in% !!district) %>%
    # dplyr::filter(BASIN %in% !!basin) %>%
    sf::st_transform(4326) %>%
    sf::st_cast("MULTIPOLYGON")
  
  doParallel::stopImplicitCluster()
  
  # pull EDDI data for each month looking at the 1 month prior, iterate over each date and input into getEDDI()
  eddi <- lapply(X = time, function(x)
    climateR::getEDDI(
      AOI         = shp,
      startDate   = x,
      timestep    = timestep,
      timescale   = "month"
      )
    ) %>%
    raster::stack()

  # mask stacks to district boundaries
  eddi <- lapply(
    X   = seq_len(nrow(shp)),
    FUN = function(x) {
      raster::mask(eddi, shp[x, ])
    }
  )

  stack_list     <- lapply(X = eddi, FUN = raster::stack)       # stack list of masked rasterstacks 
  district_names <- paste0(shp$DISTRICT)                        # district number 
  stack_list     <- setNames(stack_list, nm = district_names)   # add district names to stacks

  # create tidy tibbles from each raster stack in list, then wrangle date columns
  tidy_eddi <- lapply(X = stack_list, FUN = tidy_drought_raster) %>% 
    lapply(FUN = function(x) {
      dplyr::mutate(x,
                    year   = stringr::str_sub(date, 15, 18),
                    month = stringr::str_sub(date, 19, 20),
                    doy    = stringr::str_sub(date, 21, -1),
                    date = as.Date(with(x, paste(year,month,doy,sep="-")),"%Y-%m-%d")
      )
    })
  tidy_eddi <- lapply(
    X = names(tidy_eddi),
    FUN = function(x) {
      dplyr::mutate(
        tidy_eddi[[x]],
        district = x
      )
    }
  ) %>%
    bind_rows() %>%
    dplyr::select(lon, lat, date, district, eddi = value)
}


get_maca <- function(district, param, start_date, end_date = NULL,
                     shp_path) {
  # load shapefiles as spatial polygon object
  shp <- sf::st_read(shp_path, quiet = TRUE) %>%
    dplyr::filter(DISTRICT %in% !!district) %>%
    # dplyr::filter(BASIN %in% !!basin) %>%
    sf::st_transform(4326) %>%
    sf::st_cast("MULTIPOLYGON") 
  # dplyr::filter(DISTRICT %in% c(64))
  
  maca <- climateR::getMACA(
    shp,
    # param     = "tmax",
    # startDate = "2091-01-01",
    # endDate = "2092-01-01"
    # timeRes = "monthly",
    param     = param,
    scenario = "rcp45",
    startDate = start_date,
    endDate   = end_date
  ) 
  

  maca <- raster::stack(maca)
  
  # mask stacks to districts 
  maca_mask <- lapply(
    X   = seq_len(nrow(shp)),
    FUN = function(x) {
      raster::mask(maca, shp[x, ])
    }
  )  
  

  stack_list     <- lapply(X = maca_mask, FUN = raster::stack)       # stack list of masked rasterstacks 
  district_names <- paste0(shp$DISTRICT)                        # district number
  stack_list     <- setNames(stack_list, nm = district_names) 
  
  # create tidy tibbles from each raster stack in list, then wrangle date columns
  tidy_maca <- lapply(X = stack_list, FUN = tidy_bcca_raster) 
  
  tidy_maca <- lapply(
    X = names(tidy_maca),
    FUN = function(x) {
      dplyr::mutate(
        tidy_maca[[x]],
        district = x
      )
    }
  ) %>%
    bind_rows()
}

# loads climateR BCCA data for desired paramter rasters, masks to the specified basin, seperates data by district, and aggregates to a tidy tibble
get_bcca <- function(basin, param, start_date, end_date = NULL,
                     shp_path, rcp = "rcp45") {
  # read shapefile, filter to a basin, cast to a multipolygon for masking later on
  shp <- sf::st_read(shp_path, quiet = TRUE) %>%
    dplyr::filter(BASIN %in% !!basin) %>%
    sf::st_transform(4326) %>%
    sf::st_cast("MULTIPOLYGON")
    # dplyr::filter(DISTRICT %in% c(64))
  
  # pull gridmet data from climateR gridMET
  bcca <- climateR::getBCCA(
            AOI       = shp,
            param     = param,
            startDate = start_date,
            endDate   = end_date,
            scenario  = rcp
            )
    # param     = "prcp",
    # startDate = as.Date("2010-01-01")
    # endDate = "2010-02-01",
     # scenario = "rcp85") 
  bcca <- raster::stack(bcca[1])

  # mask stacks to districts 
  bcca_mask <- lapply(
    X   = seq_len(nrow(shp)),
    FUN = function(x) {
      raster::mask(bcca, shp[x, ])
    }
  )  

  stack_list     <- lapply(X = bcca_mask, FUN = raster::stack)       # stack list of masked rasterstacks 
  district_names <- paste0(shp$DISTRICT)                        # district number
  stack_list     <- setNames(stack_list, nm = district_names)   # add district names to stacks
  
  # create tidy tibbles from each raster stack in list, then wrangle date columns
  tidy_bcca <- lapply(X = stack_list, FUN = tidy_bcca_raster) 
  
  tidy_bcca <- lapply(
    X = names(tidy_bcca),
    FUN = function(x) {
      dplyr::mutate(
        tidy_bcca[[x]],
        district = x
      )
    }
  ) %>%
    bind_rows()
}


get_loca <- function(basin, param, start_date, end_date = NULL,
                     shp_path) {
  # load shapefiles as spatial polygon object
  shp <- sf::st_read(shp_path, quiet = TRUE) %>%
    # dplyr::filter(BASIN %in% !!basin) %>%
    sf::st_transform(4326) %>%
    sf::st_cast("MULTIPOLYGON") %>% 
    dplyr::filter(DISTRICT %in% distr_num)
  
  
  loca <- climateR::getLOCA(
    shp,
    param     = param,
    startDate = start_date,
    endDate   = end_date
  ) 
  loca <- raster::stack(loca)
  
  # mask stacks to districts 
  loca_mask <- lapply(
    X   = seq_len(nrow(shp)),
    FUN = function(x) {
      raster::mask(loca, shp[x, ])
    }
  )  
  
  
  # loca_disagg <- lapply(
  #   X   = seq_len(length(loca_mask)),
  #   FUN = function(x) {
  #     raster::disaggregate(loca_mask[[x]], fact = 4)
  #   }
  # )
  
  stack_list     <- lapply(X = loca_mask, FUN = raster::stack)       # stack list of masked rasterstacks 
  district_names <- paste0(shp$DISTRICT)                        # district number
  stack_list     <- setNames(stack_list, nm = district_names) 
  
  # create tidy tibbles from each raster stack in list, then wrangle date columns
  tidy_loca <- lapply(X = stack_list, FUN = tidy_bcca_raster) 
  
  tidy_loca <- lapply(
    X = names(tidy_loca),
    FUN = function(x) {
      dplyr::mutate(
        tidy_loca[[x]],
        district = x
      )
    }
  ) %>%
    bind_rows()
}

# loads climateR Palmer drought index rasters, aggregates to a tidy tibble
get_pdsi <- function(basin, start_date, end_date = NULL,
                     shp_path) {
  time <- seq(
    lubridate::ymd(start_date),
    lubridate::ymd(end_date),
    by = "1 day"
  )
  shp <- sf::st_read(shp_path, quiet = TRUE) %>%
    dplyr::filter(BASIN %in% !!basin) %>%
    # dplyr::filter(DISTRICT %in% !!distr) %>%
    sf::st_transform(4326) %>%
    sf::st_cast("MULTIPOLYGON")
  
  pdsi <-
    climateR::getGridMET(
      shp,
      param     = "palmer",
      startDate = start_date,
      endDate   = end_date
    ) %>%
    raster::stack()
  
  pdsi <- lapply(
    X   = seq_len(nrow(shp)),
    FUN = function(x) {
      raster::mask(pdsi, shp[x, ])
    }
  )
  stack_list     <- lapply(X = pdsi, FUN = raster::stack)
  district_names <- paste0(shp$DISTRICT)
  stack_list     <- setNames(stack_list, nm = district_names)
  tidy_pdsi <- lapply(X = stack_list, FUN = tidy_drought_raster) %>%
    lapply(FUN = function(x) {
      dplyr::mutate(
        x,
        year   = as.numeric(stringr::str_sub(date, 1, 4)),
        pentad = as.numeric(stringr::str_sub(date, 13, -1)),
        doy    = pentad * 5,
        date   = as.Date(
          doy,
          origin = paste0(year - 1, "-12-31")
        )
      )
    })
  lapply(
    X = names(tidy_pdsi),
    FUN = function(x) {
      dplyr::mutate(
        tidy_pdsi[[x]],
        district = x
      )
    }
  ) %>%
    bind_rows() %>% 
    mutate(
      wyear = lfstat::water_year(date, origin = 10)
    ) %>% 
    dplyr::select(lon, lat, date, wyear, district, value)
}
# loads climateR prcp rasters, aggregates to a tidy tibble
get_prcp <- function(basin, start_date, end_date = NULL,
                     shp_path) {
  # read shapefile, filter to a basin, cast to a multipolygon for masking later on
  shp <- sf::st_read(shp_path, quiet = TRUE) %>%
    dplyr::filter(BASIN %in% !!basin) %>%
    sf::st_transform(4326) %>%
    sf::st_cast("MULTIPOLYGON")
  
  # pull precip data from climateR gridMET
  prcp <-
    climateR::getGridMET(
      shp,
      param     = "prcp",
      startDate = start_date,
      endDate   = end_date
    ) %>%
    raster::stack()
  
  # mask stacks to districts 
  prcp <- lapply(
    X   = seq_len(nrow(shp)),
    FUN = function(x) {
      raster::mask(prcp, shp[x, ])
    }
  )
  stack_list     <- lapply(X = prcp, FUN = raster::stack)       # stack list of masked rasterstacks 
  district_names <- paste0(shp$DISTRICT)                        # district number
  stack_list     <- setNames(stack_list, nm = district_names)   # add district names to stacks
  
  # create tidy tibbles from each raster stack in list, then wrangle date columns
  tidy_prcp <- lapply(X = stack_list, FUN = tidy_raster) 
  tidy_prcp <- lapply(
    X = names(tidy_prcp),
    FUN = function(x) {
      dplyr::mutate(
        tidy_prcp[[x]],
        district = x
      )
    }
  ) %>%
    bind_rows()
}

tidy_bcca_raster <- function(raster) {
  rtable <- raster %>%
    raster::rasterToPoints() %>%
    tibble::as_tibble() %>%
    dplyr::relocate(x, y) %>%
    setNames(
      .,
      c("lon",
        "lat",
        stringr::str_sub(colnames(.)[-(1:2)], start = 2L, end = 11))
    ) %>%
    tidyr::pivot_longer(
      cols = c(tidyselect::everything(), -(1:2)),
      names_to = "date"
    ) %>%
    dplyr::mutate(date = lubridate::ymd(date)) %>%
    dplyr::relocate(lon, lat, value)
  rtable
}

tidy_drought_raster <- function(raster, mask = NULL) {
  rtable <- raster %>%
    raster::rasterToPoints() %>%
    tibble::as_tibble() %>%
    dplyr::relocate(x, y) %>%
    setNames(
      .,
      c("lon",
        "lat",
        stringr::str_sub(colnames(.)[-(1:2)], start = 2L))
    ) %>%
    tidyr::pivot_longer(
      cols = c(tidyselect::everything(), -(1:2)),
      names_to = "date"
    ) %>%
    # dplyr::mutate(date = lubridate::ymd(date)) %>%
    dplyr::relocate(lon, lat, value)
  rtable
}


# tidy each raster stack in terraclim list of Raster stacks into a tidy tibble
tidy_terra_raster <- function(raster, mask = NULL) {
  rtable <- raster %>%
    raster::rasterToPoints() %>%
    tibble::as_tibble() %>%
    dplyr::relocate(x, y) %>%
    setNames(
      .,
      c("lon",
        "lat",
        paste0(stringr::str_sub(colnames(.)[-(1:2)], start = 2L), ".1")
      )) %>%
    tidyr::pivot_longer(
      cols = c(tidyselect::everything(), -(1:2)),
      names_to = "date"
    ) %>%
    dplyr::mutate(date = lubridate::ymd(date)) %>%
    dplyr::relocate(lon, lat, value)
  rtable
}

# # tidy each raster stack in ridMET's PDSI and getEDDI() list of Raster stacks into a tidy tibble
tidy_drought_stack <- function(raster_list, as_sf = FALSE) {
  param_names <- names(raster_list)
  tidy_stacks <- lapply(X = raster_list, FUN = tidy_drought_raster)
  p <- progressr::progressor(along = param_names)
  tidy_data <-
    lapply(X = param_names,
           FUN = function(rname) {
             # p(paste0("Transforming ", rname, "..."))
             setNames(
               tidy_stacks[[rname]],
               c("lon", "lat", rname, "date")
             )
           }
    ) %>%
    purrr::reduce(dplyr::left_join, by = c("date", "lon", "lat")) %>%
    dplyr::relocate(lon, lat, date)
  if (as_sf) {
    tidy_data <-
      tidy_data %>%
      sf::st_as_sf(coords = c("lon", "lat")) %>%
      sf::st_set_crs(4326)
  }
  tidy_data
}

# tidy each raster stack in gridMET (exluding PDSI) list of Raster stacks into a tidy tibble
tidy_raster <- function(raster, mask = NULL) {
  rtable <- raster %>%
    raster::rasterToPoints() %>%
    tibble::as_tibble() %>%
    dplyr::relocate(x, y) %>%
    setNames(
      .,
      c("lon",
        "lat",
        stringr::str_sub(colnames(.)[-(1:2)], start = 2L))
    ) %>%
    tidyr::pivot_longer(
      cols = c(tidyselect::everything(), -(1:2)),
      names_to = "date"
    ) %>%
    dplyr::mutate(date = lubridate::ymd(date)) %>%
    dplyr::relocate(lon, lat, value)
  rtable
}

raster_table <- function(r) {
  rtable <- r %>%
    raster::rasterToPoints() %>%
    tibble::as_tibble() %>%
    dplyr::relocate(x, y) %>%
    setNames(
      .,
      c("lon",
        "lat",
        stringr::str_sub(colnames(.)[-(1:2)], 16, 23))
    ) %>%
    tidyr::pivot_longer(
      cols = c(tidyselect::everything(), -(1:2)),
      names_to = "date"
    ) %>%
    dplyr::mutate(date = lubridate::ymd(date)) %>%
    dplyr::relocate(lon, lat, value)
}

tidy_stack <- function(raster_list, as_sf = FALSE) {
  param_names <- names(raster_list)
  tidy_stacks <- lapply(X = raster_list, FUN = tidy_raster)
  p <- progressr::progressor(along = param_names)
  tidy_data <-
    lapply(X = param_names,
           FUN = function(rname) {
             # p(paste0("Transforming ", rname, "..."))
             setNames(
               tidy_stacks[[rname]],
               c("lon", "lat", rname, "date")
             )
           }
    ) %>%
    purrr::reduce(dplyr::left_join, by = c("date", "lon", "lat")) %>%
    dplyr::relocate(lon, lat, date)
  if (as_sf) {
    tidy_data <-
      tidy_data %>%
      sf::st_as_sf(coords = c("lon", "lat")) %>%
      sf::st_set_crs(4326)
  }
  tidy_data
}

tidy_to_raster <- function(data, x, y, z) {
  xyz <- data %>%
    dplyr::select({{ x }}, {{ y }}, {{ z }}) %>%
    dplyr::rename(
      x = {{ x }},
      y = {{ y }},
      z = {{ z }}
    )
  raster::rasterFromXYZ(
    xyz = xyz,
    crs = sf::st_crs(4326)$proj4string
  )
}

# add elevation data to climate lat/lon data
add_elev <- function(df, subdate) {
  clim <- df %>% 
    dplyr::mutate(
      year = lubridate::year(date),
      month = lubridate::month(date)
    ) %>% 
    dplyr::filter(date == subdate) 
  
  clim2 <- clim %>% 
    dplyr::select(x = lon, y = lat)
  
  pt_prj <- "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
  sp::coordinates(clim2) <- ~x+y
  sp::gridded(clim2) <- TRUE
  
  clim_sp <- sp::SpatialPoints(
    sp::coordinates(clim2),
    proj4string = sp::CRS(pt_prj)
  )
  
  elev <- elevatr::get_elev_point(
    locations = clim_sp,
    prj = pt_prj,
    src = "aws"
  ) %>% 
    sf::st_as_sf() %>% 
    dplyr::mutate(
      lon = st_coordinates(.)[,1],
      lat = st_coordinates(.)[,2]
    ) %>% 
    sf::st_drop_geometry() %>% 
    dplyr::select(-elev_units)
  
  clim_join <- dplyr::left_join(df, elev, by = c("lon", "lat"))
}

rm_collinearity <- function(model, vif_thresh = 2.5) {
  # VIF threshold. variable w/ higher VIF than threshold are dropped from the model
  threshold <- vif_thresh
  
  # Sequentially drop the variable with the largest VIF until all variables have VIF less than threshold
  drop = TRUE
  
  after_vif=data.frame()
  while(drop==TRUE) {
    vfit=car::vif(model)
    after_vif=plyr::rbind.fill(after_vif,as.data.frame(t(vfit)))
    if(max(vfit) > threshold) {
      model <- update(
        model, as.formula(paste(".","~",".","-",names(which.max(vfit))))
      ) 
    }
    else { drop=FALSE }
  }
  model
}
# aggregate, clean impacts & indicators to year timescale
aggreg_short <- function(df){
  season_short <- df %>% 
    # mutate(
    #   month = lubridate::month(date, label = TRUE),
    #   # district = as.factor(district),
    #   year = lubridate::year(date),
    #   wyear = lfstat::water_year(date)
    # ) %>% 
    filter(wyear !=2013) %>% 
    group_by(wyear, district) %>% # calculate averages during growing and not growing seasons
    summarize(
      short_total      = sum(short),
      short_dir_total  = sum(short_dir),
      demand_total     = sum(demand),
      prcp             = sum(prcp),
      soilm            = mean(soilm),
      pdsi             = mean(pdsi),
      eddi1            = mean(eddi1),
      eddi3            = mean(eddi3),
      eddi6            = mean(eddi6),
      eddi12           = mean(eddi12),
      # pdsi_gridmet     = mean(pdsi_gridmet),
      tavg             = mean(tavg),
      tmax             = mean(tmax),
      tmin             = mean(tmin),
      # spi1             = mean(spi1),
      # spi3             = mean(spi3),
      # spi6             = mean(spi6),
      # spi9             = mean(spi9),
      # spi12            = mean(spi12),
      water_deficit    = mean(water_deficit),
      aet              = mean(aet),
      pet              = mean(pet)
    ) %>% 
    ungroup() %>% 
    mutate(
      district = as.factor(district)
    )
    # mutate(
    #   season = case_when(# use case_when to create new column that assigns season values according to numb in month col 
    #     month %in% c("Apr", "May", "Jun", "Jul", "Aug", "Sep") ~"grow",
    #     # month %in% c("Dec", "Jan", "Feb", "Mar", "Oct", "Nov") ~"not_grow"
    #     month %in% c("Oct", "Nov", "Dec") ~"fall",
    #     month %in% c("Jan", "Feb", "Mar") ~"winter"
    #     # month %in% c("Mar", "Apr", "May") ~ "spring",
    #     # month %in% c("Jun", "Jul", "Aug") ~ "summer",
    #   )
    # ) %>%   
    # group_by(wyear, season, district) %>% # calculate averages during growing and not growing seasons
    # summarize(
    #   short_mean   = mean(short),
    #   prcp         = sum(prcp),
    #   soilm        = mean(soilm),
    #   pdsi_terrac  = mean(pdsi_terra),
    #   pdsi_gridmet = mean(pdsi_gridmet),
    #   tavg         = mean(tavg_c),
    #   spi1         = mean(spi1),
    #   spi3         = mean(spi3),
    #   spi6         = mean(spi6),
    #   spi9         = mean(spi9),
    #   spi12        = mean(spi12),
    #   water_def    = mean(water_deficit),
    #   aet          = mean(aet)
    # ) %>% 
    # ungroup() %>% 
    # mutate(
    #   district = as.factor(district)
    # )
}

# aggregate, clean impacts & indicators to month timescale
short_month <- function(df, basin_name) {
  shortage <- df %>% 
    mutate(
      year = lubridate::year(date),
      month = lubridate::month(date, label = TRUE), 
      # n_month  = lubridate::month(date), 
      wyear = lfstat::water_year(date)
    ) %>% 
    mutate(
      season = case_when(
        month %in% c("Sep","Oct", "Nov") ~"fall",
        month %in% c("Dec", "Jan", "Feb") ~"winter",
        month %in% c("Mar", "Apr", "May") ~ "spring",
        month %in% c("Jun", "Jul", "Aug") ~ "summer",
      ),
      season = as.factor(season),
      district = as.factor(district),
      basin = basin_name
    ) %>% 
    mutate(month = lubridate::month(date)) %>% 
    # dplyr::select(date, wyear, month, season, district, 4:20, supply, supply_dir, demand, short, short_dir) %>% 
    filter(date >= "1981-01-01") %>% 
    # rename(swe_max = swe_sum) %>%
    relocate(date, wyear, month, season, basin, district, short, short_dir, supply, supply_dir, demand, af_max, af_total, prcp) %>% 
    dplyr::select(-year) %>% 
    ungroup()
}

classify_shorts <- function(df) {
  p <- c(0, 25, 50, 75, 90, 100)/100 # quantiles
  categories <- df %>%
    group_by(district, season) %>% 
    mutate(
      short_sd = sd(short_mean),
      prcp_sd = sd(prcp)
    ) %>% 
    ungroup() %>% 
    mutate(
      prcp_class = case_when(
          prcp >= quantile(prcp)[1] & prcp < quantile(prcp)[2] ~ "low",
          prcp >= quantile(prcp)[2] & prcp < quantile( prcp)[3] ~ "medium",
          prcp >= quantile(prcp)[3] & prcp < quantile(prcp)[4] ~ "high",
          prcp >= quantile(prcp)[4] & prcp < quantile(prcp)[5] ~ "very_high"
      ),
      aet_class = case_when(
          aet >= quantile(aet)[1] & aet < quantile(aet)[2] ~ "low",
          aet >= quantile(aet)[2] & aet < quantile( aet)[3] ~ "medium",
          aet >= quantile(aet)[3] & aet < quantile(aet)[4] ~ "high",
          aet >= quantile(aet)[4] & aet <= quantile(aet)[5] ~ "very_high"
      ),
      # tavg_class = case_when(
      #     tavg_c >= quantile(tavg_c, probs = p)[1] & tavg_c < quantile(tavg_c, probs = p)[2] ~ "low",
      #     tavg_c >= quantile(tavg_c, probs = p)[2] & tavg_c < quantile(tavg_c, probs = p)[3] ~ "medium",
      #     tavg_c >= quantile(tavg_c, probs = p)[3] & tavg_c < quantile(tavg_c, probs = p)[4] ~ "high",
      #     tavg_c >= quantile(tavg_c, probs = p)[4] ~ "very_high"
      # ),
      prcp_class = as.factor(prcp_class),
      aet_class = as.factor(aet_class)
      # tavg_class = as.factor(tavg_class)
    ) %>% 
    ungroup() %>% 
    group_by(month) %>% 
    mutate(
      swe_class = case_when(
          swe_cuml >= quantile(swe_cuml, probs = p)[1] & swe_cuml < quantile(swe_cuml, probs = p)[2] ~ "low",
          swe_cuml >= quantile(swe_cuml, probs = p)[2] & swe_cuml < quantile(swe_cuml, probs = p)[3] ~ "med_low",
          swe_cuml >= quantile(swe_cuml, probs = p)[3] & swe_cuml < quantile(swe_cuml, probs = p)[4] ~ "med_high",
          swe_cuml >= quantile(swe_cuml, probs = p)[4] & swe_cuml < quantile(swe_cuml, probs = p)[5] ~ "high",
          swe_cuml >= quantile(swe_cuml, probs = p)[5] & swe_cuml <= quantile(swe_cuml, probs = p)[6] ~ "very_high"
      )
    ) %>% 
    ungroup() %>% 
    group_by(district, season) %>% 
    mutate(
      tavg_class = case_when(
        tavg_c >= quantile(tavg_c, probs = p)[1] & tavg_c < quantile(tavg_c, probs = p)[2] ~ "low",
        tavg_c >= quantile(tavg_c, probs = p)[2] & tavg_c < quantile(tavg_c, probs = p)[3] ~ "medium",
        tavg_c >= quantile(tavg_c, probs = p)[3] & tavg_c < quantile(tavg_c, probs = p)[4] ~ "high",
        tavg_c >= quantile(tavg_c, probs = p)[4] ~ "very_high"
      )
      # swe_class = as.factor(swe_cuml)
    )
}

make_shortage_wide <- function(short_df) {
  grow <- shortage %>% 
    filter(season == "grow") %>% 
    pivot_wider(names_from = "season", values_from = c(short_mean:swe_max))
  fall <- shortage %>% 
    filter(season == "fall") %>% 
    pivot_wider(names_from = "season", values_from = c(short_mean:swe_max))
  winter <- shortage %>% 
    filter(season == "winter") %>% 
    pivot_wider(names_from = "season", values_from = c(short_mean:swe_max))
  
  # join wider growing and not growing season dataframes 
  shortage <- 
    inner_join(
      grow, fall, by = c("district", "wyear")
    ) %>% 
    inner_join(
      winter, by = c("district", "wyear")
    )
  # replace NAs w/ 0s
  shortage <- shortage %>% replace(is.na(.), 0)
}

# stepwise regression on model data, either on whole data set or by individual district, direction of stepwise direction can be selected, either "forward" (default) or "backward"
step_districts <- function(df, distr = NULL, direct = "forward") {
  if (is.null(distr) & direct == "forward") {
      data <- dplyr::select(df, -wyear, -district)
      
      lm_fit <- lm(short_mean ~., data = data)
      y_int <- lm(short_mean ~ 1, data = data)
      
      lm_forward <- stats::step(y_int, direction = "forward", scope= formula(lm_fit), trace = 0)
      lm <- lm_forward$call
      form <- lm$formula
      
      lm(formula = form, data = data)
    
  } else if(is.null(distr) & direct == "backward") {
      data <- dplyr::select(df, -wyear, -district)
      
      lm_fit <- lm(short_mean ~., data = data)
      y_int <- lm(short_mean ~ 1, data = data)
      
      lm_backward <- stats::step(lm_fit, direction = "backward", scope= formula(lm_fit), trace = 0)
      lm <- lm_backward$call
      form2 <- lm$formula
      
      lm(formula = form2, data = data)
  } else if(!is.null(distr) & direct == "forward") {
      data <- df %>% 
        filter(district == distr) %>% 
        dplyr::select(-wyear, -district)
    
      lm_fit <- lm(short_mean ~., data = data)
      y_int <- lm(short_mean ~ 1, data = data)
      
      lm_forward <- stats::step(y_int, direction = "forward", scope= formula(lm_fit), trace = 0)
      lm <- lm_forward$call
      form <- lm$formula
      
      lm(formula = form, data = data)
    
  } else if(!is.null(distr) & direct == "backward") {
      data <- df %>% 
        filter(district == distr) %>% 
        dplyr::select(-wyear, -district)
      
      lm_fit <- lm(short_mean ~., data = data)
      y_int <- lm(short_mean ~ 1, data = data)
      
      lm_backward <- stats::step(lm_fit, direction = "backward", scope= formula(lm_fit), trace = 0)
      lm <- lm_backward$call
      form2 <- lm$formula
      
      lm(formula = form2, data = data)
  }
}

normalize <- function(x) {
  return ((x - min(x)) / (max(x) - min(x)))
}


# Robust scalar normalization
robust_scalar<- function(x){
  (x- median(x)) /(quantile(x,probs = .75)-quantile(x,probs = .25))
  }

# Min-Max Normalization
norm_minmax <- function(x){
  (x- min(x)) /(max(x)-min(x))
}

# Mean Normalization
mean_norm_minmax <- function(x){
  (x- mean(x)) /(max(x)-min(x))
}
aggregate_maca <- function(aoi, start_date, end_date = NULL, as_sf = FALSE) {
  p <- progressr::progressor(steps = 3L)
  p("Getting MACA data...")
  climate_data <- climateR::getMACA(
    AOI       = aoi,
    param     = common_params(),
    startDate = start_date,
    endDate   = end_date,
    model     = "BNU-ESM"
  )
  p("Tidying MACA data...")
  tidy_clim <-
    tidy_stack(
      c(climate_data),
      as_sf = as_sf
    ) 
    # dplyr::rename(
    #   prcp  = tidyselect::contains("prcp"),
    #   rhmax = tidyselect::contains("rhmax"),
    #   rhmin = tidyselect::contains("rhmin"),
    #   shum  = tidyselect::contains("shum"),
    #   srad  = tidyselect::contains("srad"),
    #   tmin  = tidyselect::contains("tmin"),
    #   tmax  = tidyselect::contains("tmax")
    # ) %>%
    # dplyr::mutate(
    #   rhavg = (rhmax + rhmin) / 2,
    #   tavg  = (tmax + tmin) / 2,
    #   cbi_rhmax_tmax = chandler_bi(rhmax, tmax),
    #   cbi_rhmin_tmax = chandler_bi(rhmin, tmax),
    #   cbi_rhavg_tmax = chandler_bi(rhavg, tmax),
    #   cbi_rhmax_tmin = chandler_bi(rhmax, tmin),
    #   cbi_rhmin_tmin = chandler_bi(rhmin, tmin),
    #   cbi_rhavg_tmin = chandler_bi(rhavg, tmin),
    #   cbi_rhmax_tavg = chandler_bi(rhmax, tavg),
    #   cbi_rhmin_tavg = chandler_bi(rhmin, tavg),
    #   cbi_rhavg_tavg = chandler_bi(rhavg, tavg),
    #   burn_index = (
    #     cbi_rhmax_tmax + cbi_rhmin_tmax + cbi_rhavg_tmax +
    #       cbi_rhmax_tmin + cbi_rhmin_tmin + cbi_rhavg_tmin +
    #       cbi_rhmax_tavg + cbi_rhmin_tavg + cbi_rhavg_tavg
    #   ) / 9
    # ) %>%
    # dplyr::select(lat, lon, date, prcp, rhmax, rhmin, shum,
    #               srad, tmin, tmax, burn_index)
  p("Tidied!")
  tidy_clim
}
# generic gridmet aggregation, use get_pdsi() if you want PDSI values
aggregate_gridmet <- function(aoi, params, start_date, end_date = NULL, as_sf = FALSE) {
  p <- progressr::progressor(steps = 3L)
  p("Getting GridMET data...")
  climate_data <- climateR::getGridMET(
    AOI       = sf::st_transform(aoi, 4326),
    param     = params,
    startDate = start_date,
    endDate   = end_date
  )
  p("Tidying GridMET data...")
  tidy_clim <-
    tidy_stack(
      c(climate_data),
      as_sf = as_sf
    ) %>%
    dplyr::rename(
      prcp       = tidyselect::contains("prcp"),
      rhmax      = tidyselect::contains("rhmax"),
      rhmin      = tidyselect::contains("rhmin"),
      shum       = tidyselect::contains("shum"),
      srad       = tidyselect::contains("srad"),
      tmin       = tidyselect::contains("tmin"),
      tmax       = tidyselect::contains("tmax"),
    )
  p("Tidied!")
  tidy_clim
}

aggregate_terraclim <- function(aoi, params, start_date, end_date = NULL, as_sf = FALSE) {
  p <- progressr::progressor(steps = 3L)
  p("Getting Terraclim data...")
  climate_data <- climateR::getTerraClim(
    AOI       = sf::st_transform(aoi, 4326),
    param     = params,
    startDate = start_date,
    endDate   = end_date
  )
  p("Tidying Terraclim data...")
  tidy_clim <-
    tidy_stack(
      c(climate_data),
      as_sf = as_sf
    ) %>%
    dplyr::rename(
      water_deficit = tidyselect::contains("water_deficit"),
      palmer         = tidyselect::contains("palmer"),
      prcp          = tidyselect::contains("prcp"),
      rhmax         = tidyselect::contains("rhmax"),
      soilm         = tidyselect::contains("soilm"),
      aet           = tidyselect::contains("aet"),
      srad          = tidyselect::contains("srad"),
      runoff        = tidyselect::contains("q"),
      swe           = tidyselect::contains("swe"),
      tmin          = tidyselect::contains("tmin"),
      tmax          = tidyselect::contains("tmax"),
    )
  p("Tidied!")
  tidy_clim
}
