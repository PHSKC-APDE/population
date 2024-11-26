#### 
#
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2021-07

#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170,
        scipen = 999)

library(tidyverse) # Manipulate data
library(dplyr) # Manipulate data
library(lubridate) # Manipulate dates
library(odbc) # Read to and write from SQL
library(RCurl) # Read files from Github
library(configr) # Read in YAML files
library(glue) # Safely combine SQL code
library(sf) # Read shape files
library(keyring) # Access stored credentials
library(stringr) # Various string functions
library(AzureStor)
library(AzureAuth)
library(svDialogs)
library(R.utils)
library(zip)
library(curl)
library(xlsx)
library(jsonlite)

devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/population/master/db_loader/create_db_connection.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/population/master/db_loader/raw_pop_functions.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/population/master/db_loader/qa_pop_functions.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/population/master/db_loader/etl_log.R")

#### USER DEFINED SETTINGS ####
temp_base <- "C:/temp/pop"
temp_zip <- paste0(temp_base, "/zip")
temp_extract <- paste0(temp_base, "/extract")
temp_gz <- paste0(temp_base, "/gz")
final_dir <- "//dphcifs/APDE-CDIP/Population/gz"
batch_name <- "20241018census2020"
memory.limit(size = 56000)
prod <- TRUE
interactive_auth <- TRUE
min_year <- 2000
etl_only <- F
census_year <- 2020

#### CONFIG FILES ####
config <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/population/master/config/common.pop.yaml")
rawconfig <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/population/master/config/raw.pop.yaml")

if(etl_only == F) {
  #### UNZIP FILES ####
  ziplist <- list.files(temp_zip)
  unlink(temp_extract, recursive = T)
  dir.create(temp_extract)
  unlink(temp_gz, recursive = T)
  dir.create(temp_gz)
  for(z in ziplist) {
    folder_name <- str_replace(z, ".zip", "")
    dir.create(paste0(temp_extract, "/", folder_name))
    dir.create(paste0(temp_gz, "/", folder_name))
    unzip(zipfile = paste0(temp_zip, "/", z), 
          exdir = paste0(temp_extract, "/", folder_name))
  }

  filelist <- list.files(temp_extract, recursive = T, include.dirs = F, full.names = T)
  data <- qa_raw_files_f(filelist = filelist)
  data$batch_name <- batch_name
  data$census_year <- census_year
  message("Review QA File Before Proceeding...")
} else {
  filelist <- list.files(final_dir, recursive = T, include.dirs = F, full.names = T)
  data <- qa_raw_files_f(server = "hhsaw", 
                         prod = prod, 
                         interactive = interactive_auth, 
                         filelist,
                         df_only = T)
  for(d in 1:nrow(data)) {
    data$batch_name[d] <- str_remove(str_remove(data$file_path[d], 
                                                data$file_name[d]),
                                     paste0(final_dir, "/"))
    data$batch_name[d] <- substring(data$batch_name[d], 1, 
                                    str_locate(data$batch_name[d], "/")[1,1] - 1)
    data$census_year[d] <- census_year
  }
}

if(etl_only == F) {
  #### CONVERT FILES TO GZ ####
  for(file in filelist) {
    gzip(file, 
        destname = paste0(str_replace(file, "extract", "gz"), ".gz"),
        remove = F)
  }

  #### LOAD TO LOCAL SERVER ####
  final_dir <- paste0(final_dir, "/", batch_name)
  unlink(final_dir, recursive = T)
  dir.create(final_dir)
  dirlist <- list.dirs(temp_gz)
  for(dir in dirlist) {
    #if(dir != temp_gz)   {
      file.copy(dir, final_dir, recursive = T)
    #}
  }
  
  #### LOAD TO AZURE ####
  blob_token <- AzureAuth::get_azure_token(
    resource = "https://storage.azure.com", 
    tenant = keyring::key_get("adl_tenant", "dev"),
    app = keyring::key_get("adl_app", "dev"),
    auth_type = "authorization_code",
    use_cache = F
  )
  blob_endp <- storage_endpoint("https://inthealthdtalakegen2.blob.core.windows.net", token = blob_token)
  cont <- storage_container(blob_endp, "inthealth")
  final_path <- paste0(rawconfig[["hhsaw"]]$base_path, "/", batch_name)
  create_storage_dir(cont, final_path)
  storage_multiupload(cont, paste0(temp_gz,"/*.*"), final_path, recursive = T)
}
    
#### ADD FILES TO ETL LOG ####
servers <- dlg_list(c("APDEStore", "hhsaw"), 
                   title = "Select Server(s) to Load ETL",
                   multiple = T,
                   preselect = c("APDEStore", "hhsaw"))$res
for(server in servers) {
  conn <- create_db_connection(server = server, interactive = interactive_auth, prod = prod)
  for(d in 1:nrow(data)) {
    data$file_loc[d] <- paste0(rawconfig[[server]]$base_url, 
                               rawconfig[[server]]$base_path, 
                               "/", data$batch_name[d])
    data$file_loc[d] <- paste0(data$file_loc[d],
                               str_remove(
                                 str_remove(
                                   str_remove(data$file_path[d], 
                                              temp_extract), 
                                   final_dir),
                                 data$file_name[d]))
    if(is.na(str_locate(data$file_name[d], ".gz")[1,1])) { 
      data$file_name[d] <- paste0(data$file_name[d], ".gz")
    }
    if(nrow(str_locate_all(data$file_loc[d], paste0("/", data$batch_name[d]))[[1]]) > 1) {
      data$file_loc[d] <- str_remove(data$file_loc[d], paste0("/", data$batch_name[d]))
    }
    
    id <- create_etl_log_f(conn,
                           server = server,
                           config = config,
                           etl_schema = "ref",
                           etl_table = "pop_metadata_etl_log",
                           batch_name = data$batch_name[d],
                           df = data[d,])
  }
  message(paste0("RAW FILE PROCESSING COMPLETE FOR SERVER: ", server))
}

