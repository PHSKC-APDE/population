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
library(configr) # Read in YAML files
library(sf) # Read shape files
library(utils)
library(dplyr)
library(survPen)
library(reticulate)
library(stringr)
library(xlsx)

### LOAD FUNCTIONS
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/population/master/db_loader/etl_log.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/population/master/db_loader/table_functions.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/population/master/db_loader/data_functions.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/population/master/db_loader/raw_pop_functions.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/population/master/db_loader/qa_pop_functions.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/population/master/db_loader/archive_pop_functions.R")

### Load config file and create path variables+
config <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/population/master/config/common.pop.yaml"))

path_raw <- config[["path_raw"]]
path_tmp <-  config[["path_tmp"]]
path_tmptxt <-  paste0(path_raw, "/tmp")
in_geo_types <- c("zip") # c("blk", "blkg", "cou", "lgd", "scd", "ste", "trc", "zip")
min_year <- 2000
memory.limit(size = 56000)

### SELECT SERVER TO USE
server <- select.list(choices = c("APDEStore", "hhsaw"))
if (server == "hhsaw") {
  ### USE PRODUCTION SERVER?
  prod_serv <- select.list(choices = c(T, F))
}

### Select the folder to qa data against what is in the database
select_qa_data_f()

### Select the folder to process and run the data processing functions
select_process_data_f()

### Create a new qa table with population sums for various column groupings
create_qa_pop_f()

warnings()

