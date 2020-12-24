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
  config,
  batch_name,
  file_name,
  geo_type,
  geo_year,
  year,
  r_type) {
  
  ### SET DATABASE SETTINGS
  etl_shema <- "metadata"
  etl_table <- "pop_etl_log"
  
  ### CHECK FOR EXISITING ENTRIES 
  ### TO DO
  #etl_match <- DBI::dbGetQuery(conn, glue::glue_sql(
  #  "SELECT TOP (1) * FROM {`etl_schema`}.{`etl_table`} ORDER BY etl_batch_id DESC",
  #  .con = conn))
  
  ### CREATE NEW ETL BATCH ID
  sql_load <- glue::glue_sql(
    "INSERT INTO {`etl_schema`}.{`etl_table`} 
    (batch_name, file_name, geo_type, geo_scope, geo_year, year, r_type) 
    VALUES ({`batch_name`}, {`file_name`}, {`geo_type`}, {`geo_scope`}, 
    {`geo_year`}, {`year`}, {`r_type`})", 
    .con = conn)
  DBI::dbGetQuery(conn, sql_load)
  
  ### GET NEW ETL BATCH ID
  etl_batch_id <- DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT TOP (1) * FROM {`etl_schema`}.{`etl_table`} 
    WHERE batch_name = {`batch_name`} AND file_name = {`file_name`} 
    AND geo_type = {`geo_type`} AND geo_scope = {`geo_scope`} 
    AND geo_year = {`geo_year`} AND year = {`year`} AND r_type = {`r_type`}
    ORDER BY etl_batch_id DESC",
    .con = conn))
  return(etl_batch_id)
}