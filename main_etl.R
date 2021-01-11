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
library(utils)
library(dplyr)
library(survPen)
library(reticulate)

### LOAD FUNCTIONS
#devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Population/master/db_loader/etl_log.R")
#devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Population/master/db_loader/table_functions.R")
#devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Population/master/db_loader/data_functions.R")
#devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/Population/master/db_loader/raw_pop_functions.R")

source(file = "C:/Users/jwhitehurst/OneDrive - King County/GitHub/Population/db_loader/etl_log.R")
source(file = "C:/Users/jwhitehurst/OneDrive - King County/GitHub/Population/db_loader/table_functions.R")
source(file = "C:/Users/jwhitehurst/OneDrive - King County/GitHub/Population/db_loader/data_functions.R")
source(file = "C:/Users/jwhitehurst/OneDrive - King County/GitHub/Population/db_loader/raw_pop_functions.R")

### Set SQL Connections
conn <- DBI::dbConnect(odbc::odbc(), "PH_APDEStore51")

### Load config file and create path variables
pop_config <- yaml::yaml.load(RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/Population/master/config/common.pop.yaml"))
raw_config <- yaml::yaml.load(RCurl::getURL("https://raw.githubusercontent.com/PHSKC-APDE/Population/master/config/raw.pop_table.yaml"))
path_raw <- pop_config[["path_raw"]]
path_tmp <-  pop_config[["path_tmp"]]
path_tmptxt <-  paste0(path_raw, "/tmp")

# Select the folder to process and run the data processing functions
select_process_data_f()

warnings()

# Set path for 7-zip
old_path <- Sys.getenv("PATH")
Sys.setenv(PATH = paste(old_path, "C:\\ProgramData\\Microsoft\\AppV\\Client\\Integration\\562FBB69-6389-4697-9A54-9FF814E30039\\Root\\VFS\\ProgramFilesX64\\7-Zip", sep = ";"))
