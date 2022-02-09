library(tidyverse)
library(here)
library(stringr)

# Name of each basin's folder where StateMod & OPR files are housed
opr_files <- list.files("data/opr/", pattern = ".opr")


# Empty list to add Tidy OPR data frames into
opr_lst <- list() 

# rm(opr_lst, opr, opr_files, names, opr_path, opr3, opr2, opr_check, opr_df, tmp, tmp2, rows_to_skip)
# i = 1
for (i in 1:length(opr_files)){
  
  logger::log_info("Cleaning OPR file - {opr_files[i]}")

  # OPR file path
  opr_path <- paste0("data/opr/", opr_files[i])
  
  # Column names for OPR file
  names <- c(
            "ID" ,       "Name",     "Admin", 
            "Str",       "On/Off",   "Dest Id", 
            "Dest Ac",   "Sou1 Id",  "Sou1 Ac",
            "Sou2 Id",   "Sou2 Ac",  "Type",  
            "ReusePlan", "Div Type", "OprLoss" ,
            "Limit",     "ioBeg",    "ioEnd"
            )
  
  # read in raw text file to find how many rows need to be skipped before data table begins
  opr_check <- readr::read_fwf(
                          file           = opr_path, 
                          col_types      = cols(),
                          col_positions  = fwf_widths(c(12, 41, 17, 9, 2, 19, 2, 19, 2, 19, 7, 3, 13, 20, 8, 8, 5, 30))
                        ) %>% 
                setNames(names)                         # set correct column names 
  
  # find row number where data table starts
  rows_to_skip <- which(
                        grepl("Name", opr_check$Name) & grepl("ID", opr_check$ID) & grepl("Admin", opr_check$Admin)
                       )[1]
  
  # read in OPR file and skip rows before table starts
  opr <- readr::read_fwf(
                          file           = opr_path, 
                          skip           = rows_to_skip,
                          col_types      = cols(),
                          comment        = "#",
                          col_positions  = fwf_widths(c(12, 39, 17, 11, 2, 17, 4, 17, 3, 19, 7, 3, 13, 20, 8, 8, 5, 30))
                          # col_positions  = fwf_widths(c(12, 41, 17, 9, 2, 19, 2, 19, 2, 19, 7, 3, 13, 20, 8, 8, 5, 30))
                        ) %>% 
                setNames(names) %>% 
                tibble() %>% 
                drop_na(ID)
  
  # add column that is Source ID and Account ID separated by a hyphen
  opr  <- opr %>%
      mutate(
          `Sou2 Id`     = case_when(
                as.character(`Sou2 Id`) == "0 0" ~ "0",
                TRUE ~  as.character(`Sou2 Id`)
                ),
          reservoir_id  = paste0(`Sou1 Id`, "-",`Sou1 Ac`),
          reservoir_id  = str_replace(reservoir_id, "-NA", ""),
          reservoir_id  = str_replace(reservoir_id, "-0", "")
        ) %>%
      janitor::clean_names()

  # convert all columns to character type
  opr[]  <- lapply(opr, as.character)
  
  # remove duplicate rows
  # unique_opr <- opr[!duplicated(opr[]),] 
  
  # Iteratively add each tidy OPR dataframe to list of OPR dataframes
  opr_lst[[i]] <- opr
}

# bind rows
opr_df <- bind_rows(opr_lst) 

# save cleaned OPR file 
write_csv(opr_df, "data/opr/opr.csv")


# clear workspace
remove(list = ls())

# write_csv(opr_df, "data/opr/opr.csv")


# *****************************************************************************************
# defunk process
# cleaning code for other for loop attempt for cleaning the OPR files
# removing commented text, removing "#", remove extraneous text
# *****************************************************************************************

# opr    <- opr %>%
#   tibble()  %>%
#   filter(!grepl("# ", ID)) %>%
#   filter(!grepl("#-----------", ID)) %>%
#   filter(ID != "#") %>%
#   mutate(ID = gsub("#", "", ID))

# # remove rows where ID is NA
# opr <- opr[!is.na(opr$ID),]

# # add column that is Source ID and Account ID separated by a hyphen
# opr <- opr %>%
#   mutate(
#     reservoir_id = paste0(`Sou1 Id`, "-",`Sou1 Ac`)
#   )

# # filter out blank rows in ID column
# opr <- subset(opr, ID != "")

# # convert all columns to character type
# opr[] <- lapply(opr, as.character)













