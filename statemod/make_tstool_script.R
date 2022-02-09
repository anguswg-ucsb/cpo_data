# Angus Watters
# 01/23/2022
# Generate TSTool script to access StateMod data

library(tidyverse)
library(stringr)
library(readr)
library(janitor)
library(readxl)
library(logger)

# ****************************************
# ------- All nodes, all baselines -------
# ****************************************

# Add path to the Excel file "210518_CPO_Data_SPDSSDist07.xlsx" 
path                    <- "C:/Users/angus/OneDrive/Desktop/lynker/CPO/StateMod/"

# all nodes all baselines data from excel sheet
# node_df <- readxl::read_xlsx(paste0(path, "210518_CPO_Data_SPDSSDist07.xlsx"), sheet = "All Nodes All Baselines") %>% 
#   setNames(.[1,])                          # assign column names from row 1 
# 
# # set names, clean data
# node_df <- node_df %>%
#   janitor::clean_names() %>%               # converts columns to snake_case, easy conversion at end, back to the excel headers 
#   slice(n = -1) %>%                        # keep all rows except the first row w/ the titles 
#   mutate(
#     from_iwr = as.numeric(from_iwr),       # numerics for filtering later
#     from_ddm = as.numeric(from_ddm)        # numerics for filtering later
#   ) 
# tmp <- node_df %>% 
#   mutate(district = case_when(
#     district <= 9 ~ paste0("0", as.character(district)),
#     TRUE          ~ as.character(district)
#   ))
# write.csv(tmp, "data/tstool/all_nodes_all_baselines.csv", row.names = F)

node_df                 <- readr::read_csv("data/tstool/all_nodes_all_baselines.csv")



# generate TSTool example excel sheet 
tstool_script           <- readr::read_csv("data/tstool/generate_tstool_script.csv")

tstool_script_template  <-  readr::read_csv("data/tstool/generate_tstool_script_template.csv")

# generate TSTool example excel sheet 
# tstool_script <- readxl::read_xlsx(paste0(path, "210518_CPO_Data_SPDSSDist07.xlsx"), sheet = 1)
# 
# # set names, clean data
# tstool_script2 <- tstool_script %>%
#   dplyr::select(6:18) %>%                  # select just cols 6:18, other columns look like fixed values that can sub into final excel sheet
#   setNames(.[1,]) %>%                      # assign col names from row 1 
#   slice(18:n())   

# ***************************************
# ---- Irrig loop (add variable col) ----
# ***************************************

# filter irrigation nodes
irrig <- node_df %>% 
  filter(type == "Irrigation") %>%
  filter(!is.na(from_iwr) & from_ddm > 0)  # removing nodes w/ NA From IWR and From DDM = 0

# unique names and unique types for irrig nodes
irrig_type   <- c("IrrigDemand", "IrrigSupply", "IrrigShortage", "IrrigSupDirectFlow")
# irrig_type   <- unique(tstool_script_template$Type)[1:4]

# empty lists to iterate through
irrig_df_lst <- list()

# split nodes into groups to iterate over
irrig_lst <- irrig %>% 
  group_by(name_description) %>% 
  group_split()

# rm(bind, df, df2)

# loop through and add type variable to irrig nodes
for (i in 1:length(irrig_lst)) {
  # iteration log info
  logger::log_info("Processing node:  {irrig_lst[[i]]$id}")
  
  irrig_type_lst <- list()
  
  df <- irrig_lst[[i]]
  
  for (k in 1:length(irrig_type)){
    # iteration log info
    log_txt <- paste0(irrig_type[k], " - (", irrig_lst[[i]]$id, ")")
    logger::log_info("{log_txt}")
  
    # select allocation type
    df2 <- df %>%
      mutate(var_type = irrig_type[k])
    
    # add to list for each node
    irrig_type_lst[[k]] <- df2
  }
  
  # bind each node allocation type
  bind <- bind_rows(irrig_type_lst)
  
  # add each processed node to list
  irrig_df_lst[[i]] <- bind
  rm(df2, df, bind, log_txt, irrig_type_lst)
}

# bind rows of irrig node list
# irrig_script <- bind_rows(irrig_df_lst)
# rm(irrig_script)

# ***********************************************
# ---- Muni & Indust loop (add variable col) ----
# ***********************************************

# filter municipal nodes
muni <- node_df %>% 
  filter(type == "Municipal")  %>% 
  filter(from_ddm > 0)                     # removing nodes where From DDM = 0

# filter industrial nodes
indust <- node_df %>% 
  filter(type == "Industrial") %>% 
  filter(is.na(from_ddm) == FALSE)         # removing nodes where From DDM is NA

# bind municipal and industrial nodes for the purpose of the next steps
muni_indust <- bind_rows(muni, indust)

# unique names and unique types for Muni/Industrial nodes
muni_type    <- c("MunDemand", "MunSupply", "MunShortage")
# muni_type   <- unique(tstool_script_template$Type)[5:7]

# empty lists to iterate through
muni_df_lst <- list()

# split nodes into groups to iterate over
muni_lst <- muni_indust %>% 
  group_by(name_description) %>% 
  group_split()

# loop through and add type variable to muni & industrial nodes
for (i in 1:length(muni_lst)) {
  # iteration log info
  logger::log_info("Processing node:  {muni_lst[[i]]$id}")
  
  muni_type_lst <- list()
  
  df <- muni_lst[[i]]
  
  for (k in 1:length(muni_type)){
    
    # empty list to add each muni node type
    # muni_type_lst <- list()
    
    # iteration log info
    log_txt <- paste0(muni_type[k], " - (", muni_lst[[i]]$id, ")")
    logger::log_info("{log_txt}")
    
    # select allocation type
    df2 <- df %>%
      mutate(var_type = muni_type[k])
    
    # add to list for each muni node
    muni_type_lst[[k]] <- df2
  }
  
  # bind each node allocation type
  bind <- bind_rows(muni_type_lst)
  
  # add each processed node to list  
  muni_df_lst[[i]] <- bind
  rm(df2, df, bind, log_txt, muni_type_lst)
}



# Combine irrigation and muni nodes to single dataframe
script <- bind_rows(
                    bind_rows(irrig_df_lst), 
                    bind_rows(muni_df_lst)
                    )
# remove extraneous data created in For loops
rm(
  muni_df_lst,  irrig_df_lst,  muni,       irrig_lst,
  irrig_script, indust, irrig, irrig_type, muni_type,
  muni_lst,     muni_indust,   i,          k
   )

# # Gut check data, compare contents of "script" variable with the data from Generate TSTool excel sheet
# unique(script$name_description)
# length(unique(script$name_description))

# unique(tstool_script_template$Name)
# length(unique(tstool_script_template$Name))

# ******************************************************
# ----------------- TSTool script page -----------------
# ******************************************************

# ***************************************************
# ---- Copy Generate TSTool Script from example  ----
# ***************************************************

final_script <- script %>%
  dplyr::select(                                                          # select needed columns, replicate column headers
    model,
    district,
    type       = var_type, 
    name       = name_description, 
    node_id    = id, 
    filename   = input_name
    ) %>% 
  mutate(
    variable = case_when(                                                 # add "Variable" column 
      type %in% c("IrrigDemand", "MunDemand")      ~ "Total_Demand",
      type %in% c("IrrigSupply", "MunSupply")      ~ "Total_Supply",
      type %in% c("IrrigShortage", "MunShortage")  ~ "Total_Short",
      type %in% c("IrrigSupDirectFlow")            ~ "From_River_By_Priority"
    )
  ) %>% 
  relocate(model, district, type, name, node_id, variable, filename) %>%  # rearrange columns 
  mutate(                                                                 # add "Extraction Template Column 
    extraction_template = 'ReadTimeSeries(TSID="NODEREPL.StateMod.VARREPL.Month~StateModB~PATHREPL",Alias="NODEREPL_TYPEREPL",IfNotFound=Warn)',
    filename            = str_replace(                                    # replace D:// w/ P:// 
                                        filename, 
                                        "D:",
                                        "P:"
                                      )
    ) 

# unique(node_df$input_name)
# unique(tstool_script_template$`Extraction Template`)

# *******************************************
# ---- Add cols N-R from Generate TSTool ----
# *******************************************

# unique(node_df$model)

# node_df %>% 
#   filter(!model %in% c("Gunnison", "Colorado", "Yampa", "White", "San Juan")) %>% 
#   filter(type == "Irrigation")
# final_script$extraction_template[1]
# final_script$model[17]
# stringr::str_replace( 
#   final_script$extraction_template[17],
#   "PATHREPL",
#   file.path("P:", "Projects", "CDSS StateMod", "SWSI 2018",  "Surface Water Models", "gm2015_SWSI",
#             "StateMod", "gm2015B.b43", fsep = "\\\\")
#   # file.path("P:", "Projects", "CDSS StateMod", "SWSI 2018",  "Surface Water Models", "SPDSS_FullModel",
#   #           "SP2016_08172017", "StateMod", "SP2016_SWSI_B.b43", fsep = "\\\\")         # South Platte path 
# )

final_script <- final_script %>%
  mutate(
    pathrepl = 
      case_when( # each basin/model has its own unique path string 
        model == "South Platte" ~ stringr::str_replace(                                                                  # South Platte path 
                                    final_script$extraction_template,
                                    "PATHREPL",
                                    file.path("P:", "Projects", "CDSS StateMod", "SWSI 2018",  "Surface Water Models", "SPDSS_FullModel",
                                              "SP2016_08172017", "StateMod", "SP2016_SWSI_B.b43", fsep = "\\\\")      
                                    ),
        model == "Colorado" ~ stringr::str_replace(                                                                      # Colorado path 
                                    final_script$extraction_template,
                                    "PATHREPL",
                                    file.path("P:", "Projects", "CDSS StateMod", "SWSI 2018",  "Surface Water Models", "cm2015_SWSI",
                                              "StateMod", "cm2015B.b43", fsep = "\\\\")                               
                                    ),
        model == "Gunnison" ~ stringr::str_replace(                                                                      # Gunnison path 
                                    final_script$extraction_template,
                                    "PATHREPL",
                                    file.path("P:", "Projects", "CDSS StateMod", "SWSI 2018",  "Surface Water Models", "gm2015_SWSI",
                                              "StateMod", "gm2015B.b43", fsep = "\\\\")                                
                                    ),
        model == "San Juan" ~ stringr::str_replace(                                                                      # San Juan path 
                                    final_script$extraction_template,
                                    "PATHREPL",
                                    file.path("P:", "Projects", "CDSS StateMod", "SWSI 2018",  "Surface Water Models", "sj2015_SWSI",
                                              "StateMod", "sj2015B.b43", fsep = "\\\\")                                 
                                    ),
        model == "White" ~ stringr::str_replace(                                                                         # White path
                                    final_script$extraction_template, 
                                    "PATHREPL",
                                    file.path("P:", "Projects", "CDSS StateMod", "SWSI 2018",  "Surface Water Models", "wm2015_SWSI",
                                              "StateMod", "wm2015B.b43", fsep = "\\\\")                                  
                                    ),
        model == "Yampa" ~ stringr::str_replace(                                                                         # Yampa path
                                    final_script$extraction_template,
                                    "PATHREPL",
                                    file.path("P:", "Projects", "CDSS StateMod", "SWSI 2018",  "Surface Water Models", "ym2015_SWSI",
                                              "StateMod", "ym2015B.b43", fsep = "\\\\")                                  
                                    )
        )
    ) %>% 
  mutate(
        noderepl = stringr::str_replace(    # replace 1st instance of "NODEREPL" text 
             pathrepl,                      # build previously made pathrepl column (above), this time replacing the 1st NODEREPL text
             "NODEREPL",  
             node_id
             ),
        noderepl =stringr::str_replace(     # replace 2nd instance of "NODEREPL" text, definitely a way of doing this in one function call but this was quicker
             noderepl,                      # build first instance of the noderepl column (above), this time replacing the 2nd NODEREPL text
             "NODEREPL_",  
             paste0(node_id, "_")
             )
    ) %>% 
  mutate(
        varrepl = stringr::str_replace(     # replace "VARREPL" text 
             noderepl,                      # build noderepl column (above), this time replacing the VARREPL text
             "VARREPL",  
             variable
             )
    ) %>% 
  mutate(
        typerepl = stringr::str_replace(    # replace "TYPEREPL" text 
             varrepl,                       # build varrepl column (above), this time replacing the TYPEREPL text
             "TYPEREPL",  
             type
             )
    ) %>% 
  mutate(
        final = typerepl                    # duplicate the typerepl column, which is how the excel sheet has it
  ) 

# # save/read output
write.csv(final_script, "data/tstool/final_tstool_script_template.csv", row.names = F)
# final_script  <- readr::read_csv("data/tstool/final_tstool_script_template.csv")


# # cross checking output w/ excel sheet example 
# final_script$extraction_template
# final_script$pathrepl[1]
# final_script$noderepl[1]
# final_script$noderepl2[1]
# final_script$typerepl[1]


# **************************************
# ---- NewTimeSeries & Add() functs ----
# **************************************

# fixed variables to call upon
template_script <- tstool_script %>% 
  setNames(.[1,])  %>% 
  janitor::clean_names() %>%
  dplyr::select(1:2) %>%
  slice(1:17) %>% 
  na.omit() %>% 
  filter(type != "#")


# Add TS function

# dataframe for Add() function templates
add_func <- tstool_script_template[c(91:97),] %>% 
  janitor::clean_names() %>%
  dplyr::select(type, variable, extraction_template)
add_func <- readr::read_csv("data/tstool/addts_tstool_script.csv")

# Iterate through districts and make 7 rows for the 7 variable types
vars_lst <- list()

for (i in 1:length(add_func$variable)){
  
  add_vars <- final_script %>% 
    dplyr::select(district) %>% 
    group_by(district) %>% 
    slice(n = 1) %>% 
    mutate(variable = add_func$variable[i]) 
  
  vars_lst[[i]] <- add_vars
}

add_ts <- bind_rows(vars_lst) %>% 
  ungroup()

rm(add_vars, vars_lst)

# join districts/variables w/ extraction template
add_ts <- add_ts %>% 
  left_join(add_func, by = "variable") %>% 
  dplyr::relocate(district, type, variable, extraction_template)

# add_ts$extraction_template[1]

# fill in template for AddTS() 
add_ts <- add_ts %>% 
  mutate(
      district   = as.character(district),
      pathrepl = extraction_template
      ) %>%
  mutate(
      noderepl = stringr::str_replace_all(    # replace 1st instance of "NODEREPL" text 
          pathrepl,                           # build previously made pathrepl column (above), this time replacing the 1st NODEREPL text
          "NODEREPL",  
          district
          )
      ) %>% 
  mutate(
      varrepl = stringr::str_replace_all(    # replace 1st instance of "NODEREPL" text 
          noderepl,                      # build previously made pathrepl column (above), this time replacing the 1st NODEREPL text
          "VARREPL",  
          variable
          )
      )%>% 
  mutate(
      typerepl = stringr::str_replace_all(    # replace 1st instance of "NODEREPL" text 
          varrepl,                      # build previously made pathrepl column (above), this time replacing the 1st NODEREPL text
          "TYPEREPL",  
          type
          )
      ) %>% 
  mutate(
      final = typerepl
      ) %>% 
  dplyr::select(-type) %>% 
  rename(type = variable) %>% 
  dplyr::relocate(district, type)

# save output
write.csv(
          add_ts,
          "data/tstool/addts_tstool_script.csv",
          row.names	= FALSE
          )


# *******************************
# ---- NewTimeSeries() funct ----
# *******************************

# fixed variables to call upon
template_script <- tstool_script %>% 
  setNames(.[1,])  %>% 
  janitor::clean_names() %>%
  dplyr::select(1:2) %>%
  slice(1:17) %>% 
  na.omit() %>% 
  filter(type != "#")

# newTS variable
new_ts_func <- tstool_script %>% 
  dplyr::select(6, 10, 13:18) %>%                  # select just columns 6:18, the other columns look like fixed values that we can sub into our final excel sheet
  setNames(.[1,]) %>%                      # assign column names from row 1 
  janitor::clean_names() %>%
  slice(4:10) %>% 
  dplyr::select(type, variable, extraction_template)

# Iterate through districts and make 7 rows for the 7 variable types
new_ts_var_lst <- list()

for (i in 1:length(new_ts_func$variable)){
  
  logger::log_info("filling NewTimeSeries functions - {new_ts_func$variable[i]}")

  new_ts_var <- final_script %>% 
    dplyr::select(district) %>% 
    group_by(district) %>% 
    slice(n = 1) %>% 
    mutate(variable = new_ts_func$variable[i]) 
  
  new_ts_var_lst[[i]] <- new_ts_var
}

new_ts <- bind_rows(new_ts_var_lst) %>% 
  ungroup()

# join districts/variables w/ extraction template
new_ts <- new_ts %>% 
  left_join(new_ts_func, by = "variable") %>% 
  dplyr::relocate(district, type, variable, extraction_template)

# new_ts$extraction_template[1]

# fill in template for NewTimeSeries() 
new_ts <- new_ts %>% 
  mutate(
    district = as.character(district),
    pathrepl = extraction_template
  ) %>%
  mutate(
    noderepl = stringr::str_replace_all(    # replace 1st instance of "NODEREPL" text 
      pathrepl,                           # build previously made pathrepl column (above), this time replacing the 1st NODEREPL text
      "NODEREPL",  
      district
    )
  ) %>% 
  mutate(
    varrepl = stringr::str_replace_all(    # replace 1st instance of "NODEREPL" text 
      noderepl,                      # build previously made pathrepl column (above), this time replacing the 1st NODEREPL text
      "VARREPL",  
      variable
    )
  ) %>% 
  mutate(
    typerepl = stringr::str_replace_all(    # replace 1st instance of "NODEREPL" text 
      varrepl,                      # build previously made pathrepl column (above), this time replacing the 1st NODEREPL text
      "TYPEREPL",  
      type
    )
  ) %>% 
  mutate(
    final = typerepl
  ) %>% 
  dplyr::select(-type) %>% 
  rename(type = variable) %>% 
  dplyr::relocate(district, type)

rm(new_ts_func, new_ts_df, new_ts_var, new_ts_var_lst, add_func)
# example file
# t <- 'Add(TSID="07_AGG_VARREPL",AddTSList=AllMatchingTSID,AddTSID="07*VARREPL*",HandleMissingHow="IgnoreMissing")'


# save output
write.csv(
        new_ts,
        "data/tstool/newts_tstool_script.csv", 
        row.names	= FALSE
        )

# ************************************
# ---- WriteDelimitedFiles funct -----
# ************************************

# WriteDeliminitedFile variable template
write_delim_txt <- 'WriteDelimitedFile(TSList=AllTS,TSID="NODEREPL*",OutputFile="PATHREPL",ValueColumns="%A")'

csv_write <- add_ts %>% 
  dplyr::select(district, type) %>% 
  mutate(
      extraction_template = write_delim_txt
      # destination_path    = write_path
      ) %>% 
  mutate(
      pathrepl = stringr::str_replace(    # replace 1st instance of "NODEREPL" text 
          extraction_template,                           # build previously made pathrepl column (above), this time replacing the 1st NODEREPL text
          "PATHREPL",  
          file.path("statemod_output_district_NODEREPL.csv", fsep = "\\\\")  
          # file.path("210125_StateModOutput_DistNODEREPL.csv", fsep = "\\\\")  
          # file.path("P:", "Projects", "CDSS StateMod", "Extraction",  "210125_StateModOutput_DistNODEREPL.csv", fsep = "\\\\")  
          # file.path("P:", "Projects", "CRWAS StateMod", "Extraction",  "210125_StateModOutput_DistNODEREPL.csv", fsep = "\\\\")  
          )
      ) %>%  
  mutate(
      noderepl = stringr::str_replace_all(    # replace 1st instance of "NODEREPL" text 
          pathrepl,                           # build previously made pathrepl column (above), this time replacing the 1st NODEREPL text
          "NODEREPL",  
          district
          )
      ) %>% 
  mutate(
      varrepl = stringr::str_replace_all(    # replace 1st instance of "NODEREPL" text 
          noderepl,                      # build previously made pathrepl column (above), this time replacing the 1st NODEREPL text
          "VARREPL",  
          type
          )
      ) %>% 
  mutate(
      typerepl = stringr::str_replace_all(    # replace 1st instance of "NODEREPL" text 
          varrepl,                      # build previously made pathrepl column (above), this time replacing the 1st NODEREPL text
          "TYPEREPL",  
          type
        )
      ) %>% 
  mutate(
    final = typerepl
      ) %>% 
  dplyr::relocate(district, type)


# save output
write.csv(
        csv_write, 
        "data/tstool/writedelimitedfile_tstool_script.csv", 
        row.names	= FALSE
        )

# gut check results
# csv_write$extraction_template[1]
# cat(csv_write$pathrepl[1])
# cat(csv_write$final[1])

# ****************************************************
# ---- build CSV file template for TSTool to read ----
# ****************************************************
# add_ts <- readr::read_csv("data/tstool/addts_tstool_script.csv", 
#                            col_names = T,
#                            col_types = cols(
#                                            district            = col_character(),
#                                            type                = col_character(),
#                                            variable            = col_character(),
#                                            extraction_template = col_character(),
#                                            pathrepl            = col_character(),
#                                            noderepl            = col_character(),
#                                            varrepl             = col_character(),
#                                            typerepl            = col_character(),
#                                            final               = col_character()
#                                          )
#                            )


csv_write     <- readr::read_csv("data/tstool/writedelimitedfile_tstool_script.csv")
new_ts        <- readr::read_csv("data/tstool/newts_tstool_script.csv")
add_ts        <- readr::read_csv("data/tstool/addts_tstool_script.csv")
final_script  <- readr::read_csv("data/tstool/final_tstool_script_template.csv")
final_script  <- final_script %>% 
  mutate(district = case_when(
    district < 10 ~ paste0("0", as.character(district)),
    TRUE ~ as.character(district)
  )
  )

# WriteDelimitedFile function
csv_write_final  <- csv_write %>% 
  dplyr::select(district, type, final) %>% 
  group_by(district) %>% 
  slice(n = 1) %>% 
  ungroup() 

# NewTimeseries function
new_ts_final     <- new_ts %>%
  dplyr::select(district, type, final) 

# Add function
add_ts_final     <- add_ts %>% 
  dplyr::select(district, type, final) 

# ReadTimeseries function
read_ts_final    <- final_script %>% 
  dplyr::select(model:variable, final) 

# SetOutputPeriod function
set_output_period <- data.frame(final = 'SetOutputPeriod(OutputStart="10/1970",OutputEnd="12/2012")')

# *******************************************
# ---- make & save TSTool command files  ----
# *******************************************

# Join all parts of TSTool script
txt_script <- bind_rows(new_ts_final, read_ts_final,  add_ts_final, csv_write_final) %>% 
  mutate(final = as.factor(final)) 

# split script by district
script_split <- txt_script %>% 
  group_by(district) %>% 
  group_split()

script_lst <- list()

# For loop makes unique TSTool command script for each district & saves to project directory
for (i in 1:length(script_split)) {
  
  logger::log_info("Generating TSTool command file - District {script_split[[i]]$district[1]}")
  
  # tstool_path <- "C:/Users/angus/OneDrive/Desktop/lynker/CPO/TSTool"
  
  dist <- script_split[[i]]$district[1]
  # t <-  dist <- script_split[[1]]$type[1]
  
  out <- script_split[[i]] %>% 
    dplyr::select(final)
  
  
  # SetOutputPeriod function
  set_output_period <- data.frame(final = 'SetOutputPeriod(OutputStart="10/1970",OutputEnd="12/2012")')
  
  # 
  out <- bind_rows(set_output_period, out) %>% 
    setNames(c("# #"))
  
  # save TSTool command file to project directory
  write.table(
      out,
      file      = paste0("data/tstool/scripts/tstool_script_district_", dist, ".TSTool"),
      row.names = FALSE,
      quote     = FALSE
      )
  
  rm(out, dist, set_output_period)
  }

# ********************************************************************

# Currently, the commands you've generated only read in the StateMod timeseries (i.e., ReadTimeSeries function).
# I believe there are several other important steps, which come before (1) and after (2 &3) the commands you've already generated:

# 1) populate an empty variable across a set time period (SetOutputPeriod and NewTimeSeries functions)
# 2) explicitly operate on these time series, adding them to the above variables (Add function)
# 3) lastly, save all data out to a CSV (WriteDelimitedFiles function).
# 
# This set of commands bracket every individual district- so that we're generating new variables, reading in the correct timeseries, adding those timeseries up to those variables, and saving them out to a CSV each time. I think I have a pretty slick way to do this in Excel, that uses what you've generated already, but we should strategize- perhaps tomorrow or later this week







