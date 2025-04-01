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
library(jsonlite)
library(data.table)

devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/master/R/create_table.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/apde/master/R/load_table_from_file.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/population/master/db_loader/copy_into.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/population/master/db_loader/create_db_connection.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/population/master/db_loader/raw_pop_functions.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/population/master/db_loader/stage_pop_functions.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/population/master/db_loader/archive_pop_functions.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/population/master/db_loader/data_functions.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/population/master/db_loader/table_functions.R")
devtools::source_url("https://raw.githubusercontent.com/PHSKC-APDE/population/master/db_loader/etl_log.R")

#### SELECT CONNECTION TYPE ####
server <- "hhsaw"
server_dw <- "inthealth"
prod <- T
interactive_auth <- F

conn_db <- create_db_connection(server, interactive = interactive_auth, prod = prod)
conn_dw <- create_db_connection(server_dw, interactive = interactive_auth, prod = prod)

#### CONFIG FILES ####
popconfig <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/population/master/config/common.pop.yaml")
rawconfig <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/population/master/config/raw.pop.yaml")

#### DEFINE LOADING VARIABLES ####
in_geo_types <- dlg_list(c("blk", "blkg", "cou", "lgd", "scd", "ste", "trc", "zip"), 
                         multiple = T,
                         preselect = c("blk", "scd", "zip"),
                         title = "Select GEO Types to Load")$res
census <- dlg_list(c("2010", "2020"), 
                   title = "Select Census Year")$res
min_year <- 2000
base_path <- rawconfig[[server]]$base_path
base_url <- rawconfig[[server]]$base_url
raw_schema <- popconfig[[server]]$raw_schema
stage_schema <- popconfig[[server]]$stage_schema
ref_schema <- popconfig[[server]]$ref_schema
archive_schema <- popconfig[[server]]$archive_schema
raw_table <- paste0(popconfig[[server]]$raw_prefix,
                    popconfig[[server]]$table_name)
stage_table <- paste0(popconfig[[server]]$stage_prefix,
                     popconfig[[server]]$table_name)
ref_table <- paste0(popconfig[[server]]$ref_prefix,
                    popconfig[[server]]$table_name)
archive_table <- paste0(popconfig[[server]]$archive_prefix,
                        popconfig[[server]]$table_name)
etl_table <- popconfig$etl_table

#### SELECT BATCH/FOLDERS TO LOAD ####
batch_list <- get_etl_list_to_load_f(conn_db,
                                     server = server,
                                     etl_schema = ref_schema,
                                     etl_table = popconfig$etl_table,
                                     geo_types = in_geo_types,
                                     census_year = census,
                                     min_year = min_year,
                                     base_path = base_path,
                                     base_url = base_url)
batch <- dlg_list(batch_list$batch_name, title = "Select Batch to Load")$res

if (dlg_list(c("TRUE", "FALSE"), title = "Load Entire Batch?")$res == F) {
  folder_list <- get_etl_list_to_load_f(conn_db,
                                        server = server, 
                                        etl_schema = ref_schema,
                                        etl_table = popconfig$etl_table,
                                        geo_types = in_geo_types,
                                        census_year = census,
                                        min_year = min_year,
                                        base_path = base_path,
                                        base_url = base_url,
                                        batch_name = batch,
                                        get_folders = T)[,1]
  folder_list <- substring(folder_list, 1, nchar(folder_list) - 1)
  subs <- as.list(dlg_list(folder_list, multiple = T, title = "Select Folder to Load")$res)
} else {
  subs <- c()
}
conn_db <- create_db_connection(server, interactive = interactive_auth, prod = prod)
files <- get_etl_list_to_load_f(conn_db,
                                server = server, 
                                etl_schema = ref_schema,
                                etl_table = popconfig$etl_table,
                                geo_types = in_geo_types,
                                census_year = census,
                                min_year = min_year,
                                base_path = base_path,
                                base_url = base_url,
                                batch_name = batch,
                                folders = subs)
files <- files[files$r_type == 97,]
DBI::dbDisconnect(conn_db)
message(paste0("File(s) to process: ", nrow(files)))

#### PROCESS FILES #### 
for (f in 1:nrow(files)) {
  
  message(glue("### Processing file {f} of {nrow(files)} - {format(Sys.time(), '%Y-%m-%d %H:%M:%S')} ###"))
  conn_dw <- create_db_connection(server_dw, interactive = interactive_auth, prod = prod)
  file <- files[f,]
  if (file$geo_type %in% in_geo_types == FALSE || file$year < min_year) { next }
  message(glue("ETL Batch ID: {file$id} - {file$file_loc}{file$file_name}"))
  message(glue("...Loading Raw Data to Raw Table ({raw_schema}.{raw_table}) - {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}"))
  suppressMessages(load_raw_f(server = server,
             server_dw = server_dw,
             prod = prod,
             interactive_auth = interactive_auth,
             config = rawconfig,
             schema_name = raw_schema,
             table_name = raw_table,
             file_path = paste0(file$file_loc, file$file_name),
             etl_batch_id = file$id))
#  message(glue("...Cleaning Raw Data - {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}"))
#  clean_raw_sql_f(server = server,
#                  server_dw = server_dw,
#                  prod = prod,
#                  interactive_auth = interactive_auth,
#                  schema_name = raw_schema,
#                  table_name = raw_table,
#                  vars_add = rawconfig$vars_add,
#                  info = file,
#                  etl_batch_id = file$id)
  message(glue("...Moving Data from Raw Table ({raw_schema}.{raw_table}) to Stage Table ({stage_schema}.{stage_table}) - {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}"))
  load_stage_f(server = server,
               server_dw = server_dw,
               prod = prod,
               interactive_auth = interactive_auth,
               raw_schema = raw_schema,
               stage_schema = stage_schema,
               raw_table = raw_table,
               stage_table = stage_table,
               etl_batch_id = file$id)
#  message(glue("...Cleaning Stage Data - {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}"))
#  clean_stage_f(server = server,
#                server_dw = server_dw,
#                prod = prod,
#                interactive_auth = interactive_auth,
#                schema_name = stage_schema,
#                table_name = stage_table,
#                xwalk_schema = ref_schema,
#                xwalk_table = popconfig$crosswalk_table,
#                hra_table = popconfig$hra_table,
#                etl_batch_id = file$id,
#                info = file)
  if (file$r_type == 77) { 
    to_ref <- paste0(ref_table, "_77")
  } else { 
    to_ref <- ref_table 
  }
#  message(glue("...Moving Data from Stage Table ({stage_schema}.{stage_table}) to Ref Table ({ref_schema}.{to_ref}) - {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}"))
#  conn_db <- create_db_connection(server, interactive = interactive_auth, prod = prod)
#  if(server == "hhsaw") {
#    ext_schema <- ref_schema
#  } else {
#    ext_schema <- stage_schema
#  }
#  data_move_f(conn_db,
#              from_schema = ext_schema,
#              to_schema = ref_schema,
#              from_table = stage_table,
#              to_table = to_ref,
#              etl_batch_id = file$id,
#              del_to = T)
  conn_db <- create_db_connection(server, interactive = interactive_auth, prod = prod)
  qa_results <- get_row_pop_f(conn_db, ref_schema, to_ref, file$id)
  update_etl_log_datetime_f(conn = conn_db, etl_batch_id = file$id,
                            etl_schema = ref_schema, etl_table = etl_table,
                            field = "load_ref_datetime")
  qa_etl_f(conn = conn_db, etl_batch_id = file$id,
           etl_schema = ref_schema, etl_table = etl_table,
           qa_val = qa_results$row_cnt, field = "qa_rows_ref")
  qa_etl_f(conn = conn_db, etl_batch_id = file$id,
           etl_schema = ref_schema, etl_table = etl_table,
           qa_val = qa_results$pop_tot, field = "qa_pop_ref")
  DBI::dbDisconnect(conn_db)
  DBI::dbDisconnect(conn_dw)
}





#### STAGE TABLE TO FINAL TABLE ####
table_list <- c("pop_geo_blk",
                "pop_geo_CCL",
                "pop_geo_cou",
                "pop_geo_CSA",
                "pop_geo_hra",
                "pop_geo_Inc_uninc",
                "pop_geo_kccd",
                "pop_geo_lgd",
                "pop_geo_PUMA",
                "pop_geo_reg",
                "pop_geo_scd",
                "pop_geo_tribal",
                "pop_geo_zip")

message(paste0("Beginning process to copy data from INTHEALTH_EDW to HHSAW - ", Sys.time()))

#Begin loop
for(table in table_list) {
  from_schema <- "stg_reference" 
  from_table <- table
  to_schema <- "ref"
  to_table <- table
  ext_schema <- "ref"
  ext_table <- paste0("stage_", table)
  
  message(paste0("Working on table: ", to_table, " - ", Sys.time()))
  conn_db <- create_db_connection("hhsaw", interactive = interactive_auth, prod = prod)
  conn_dw  <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)
  if(prod == T) {
    server_to <- "hhsaw"
  } else {
    server_to <- "hhsaw_dev"
  }
  
    DBI::dbExecute(conn = conn_db,
                 glue::glue_sql("execute ref.usp_external_table_load @fromtable = N{ext_table}, @totable = N{to_table};",
                                .con = conn_db))
  
  #Row count comparison for all tables except PLR tables
  inthealth_row_count <- DBI::dbGetQuery(conn = conn_dw,
                                         glue::glue_sql("select count(*) as row_count from {`from_schema`}.{`from_table`};",
                                                        .con = conn_dw))
  hhsaw_row_count <- DBI::dbGetQuery(conn = conn_db,
                                     glue::glue_sql("select count(*) as row_count from {`to_schema`}.{`to_table`};",
                                                    .con = conn_db))
  
  if (inthealth_row_count$row_count == hhsaw_row_count$row_count) {
    message(paste0("Table Transfer: ", to_table, " - PASS ", Sys.time()))  
  } else {
    message(paste0("Table Transfer: ", to_table, " - FAIL ", Sys.time()))  
  }
  
  
}

## Closing message
message(paste0("All tables have been successfully copied from inthealth_edw - ", Sys.time()))





























#### ARCHIVE AND DELETE OLD DATA ####
message("Archiving Old Data")
load_archive_f(server = server,
               prod = prod,
               interactive_auth = interactive_auth,
               archive_schema = archive_schema,
               archive_table = archive_table,
               ref_schema = ref_schema,
               ref_table = ref_table,
               etl_schema = ref_schema,
               etl_table = etl_table)
message("Cleaning Out Old Archive Data")
clean_archive_f(server = server,
               prod = prod,
               interactive_auth = interactive_auth,
               archive_schema = archive_schema,
               archive_table = archive_table,
               etl_schema = ref_schema,
               etl_table = etl_table)
