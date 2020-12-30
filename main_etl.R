#### MAIN ETL CODE FOR LOADING POPULATION DATA
#
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2020-12

#### Set up global parameter and call in libraries ####
options(max.print = 350, tibble.print_max = 50, warning.length = 8170, scipen = 999)
library(odbc) # Read to and write from SQL
library(tidyverse) # Manipulate data
library(lubridate) # Manipulate dates
library(glue) # Safely combine SQL code
library(RCurl) # Read files from Github
library(configr) # Read in YAML files
library(sf) # Read shape files
library(zip) # Unzip files
library(dplyr)
library(survPen)

### LOAD FUNCTIONS
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Population/master/db_loader/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Population/master/db_loader/table_functions.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Population/master/db_loader/data_functions.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Population/master/db_loader/raw_pop_functions.R")

### Set SQL Connections
conn <- DBI::dbConnect(odbc::odbc(), "PH_APDEStore51")

### Load config file and create path variables
pop_config <- yaml::yaml.load(RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/Population/master/config/common.pop.yaml"))
raw_config <- yaml::yaml.load(RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/Population/master/config/raw.pop_table.yaml"))
path_raw <- pop_config[["path_raw"]]
path_tmp <-  pop_config[["path_tmp"]]
path_tmptxt <-  paste0(path_raw, "/tmp")

# List of folders that have raw data
f_list <- list.dirs(path = path_raw, full.names = F, recursive = F)
f_list <- f_list[ f_list != "tmp"]

### CHOOSE THE FOLDER TO LOAD RAW DATA FROM ###
message("CHOOSE THE FOLDER TO LOAD RAW DATA FROM")
f_load <- select.list(choices = f_list)

load_data_f(conn = conn, pop_config = pop_config, raw_config = raw_config,
  path_raw = path_raw, path_tmp = path_tmp, path_tmptxt = path_tmptxt, 
  f_load = f_load)

rm(f_load, conn, pop_config, raw_config, path_raw, path_tmp, path_tmptxt, f_list)

