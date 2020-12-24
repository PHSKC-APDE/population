#### FUNCTIONS TO CREATE AND MODIFY ETL LOGS
#
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2020-12

#### PARAMETERS ####
# conn = name of the connection to the SQL database
# 


#### FUNCTION CREATE ETL LOG ####
create_etl_log_f <- function(
  conn,
  batch_name,
  file_name,
  geo_type,
  geo_year,
  year) {
  
  ### SET DATABASE SETTINGS
  etl_shema <- "metadata"
  etl_table <- "pop_etl_log"
  
  ### CHECK FOR EXISITING ENTRIES
   <- DBI::dbGetQuery(conn, 
                            glue::glue_sql("SELECT TOP (1) * FROM {`schema`}.{`table`}
                             ORDER BY etl_batch_id DESC",
                                           .con = conn))
  
  
}