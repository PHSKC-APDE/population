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

### Set SQL Connections
conn <- DBI::dbConnect(odbc::odbc(), "PH_APDEStore51")

### Create path variables
path_informatica <- "//kcitetldepim001/Informatica/APDE/Population"
path_raw <- "//phdata01/DROF_DATA/DOH DATA/POP/data/raw"
path_tmp <-  paste0(path_raw, "/tmp")
# Local tmp folder
path_tmp <-  "C:/temp/pop" 

# List of folders that have raw data
f_list <- list.dirs(path = path_raw, full.names = F, recursive = F)
f_list <- f_list[ f_list != "tmp"]

### CHOOSE THE FOLDER TO LOAD RAW DATA FROM ###
message("CHOOSE THE FOLDER TO LOAD RAW DATA FROM")
f_load <- select.list(choices = f_list)

### Create list of zip files with csvs in raw data folder
zipped_files <- as.data.frame(list.files(paste0(path_raw, "/", f_load), pattern = c("csv_", ".zip")))

### Begin loop to extract data to tmp folder, archive old sql tables, 
### prep new sql tables, insert data to sql, and remove tmp data - one zip file at a time
stime <- Sys.time()
for (i in 1:nrow(zipped_files)) {
  unzip(paste0(path_raw, "/", f_load, "/", zipped_files[i,]), exdir = path_tmp)
  
  
}
etime <- Sys.time()
etime - stime

rm(conn, list = ls(pattern = "path"), f_load, f_list, i, zipped_files)
