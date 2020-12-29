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
  geo_scope,
  geo_year,
  year,
  r_type,
  qa_rows_file) {
  
  ### SET DATABASE SETTINGS
  etl_schema <- "metadata"
  etl_table <- "pop_etl_log"
  
  ### CREATE NEW ETL BATCH ID
  sql_load <- glue::glue_sql(
    "INSERT INTO {`etl_schema`}.{`etl_table`} 
    (batch_name, file_name, geo_type, geo_scope, geo_year, year, r_type, qa_rows_file) 
    VALUES ({batch_name}, {file_name}, {geo_type}, {geo_scope}, 
    {geo_year}, {year}, {r_type}, {qa_rows_file})", 
    .con = conn)
  DBI::dbGetQuery(conn, sql_load)
  
  ### GET NEW ETL BATCH ID
  if (is.na(geo_scope)) {
    sql_get <- glue::glue_sql(
      "SELECT TOP (1) id FROM {`etl_schema`}.{`etl_table`} 
      WHERE batch_name = {batch_name} AND file_name = {file_name} 
      AND geo_type = {geo_type} AND geo_scope IS NULL
      AND geo_year = {geo_year} AND year = {year} AND r_type = {r_type}
      ORDER BY id DESC",
      .con = conn)
  }
  else {
    sql_get <- glue::glue_sql(
      "SELECT TOP (1) id FROM {`etl_schema`}.{`etl_table`} 
      WHERE batch_name = {batch_name} AND file_name = {file_name} 
      AND geo_type = {geo_type} AND geo_scope = {geo_scope}
      AND geo_year = {geo_year} AND year = {year} AND r_type = {r_type}
      ORDER BY id DESC",
      .con = conn)
  }
  etl_batch_id <- DBI::dbGetQuery(conn, sql_get)
  return(etl_batch_id)
}

#### FUNCTION UPDATE ETL LOG DATETIME FIELD ####
update_etl_log_datetime_f <- function(
  conn,
  etl_batch_id,
  field) {

  ### SET DATABASE SETTINGS
  etl_schema <- "metadata"
  etl_table <- "pop_etl_log"
  
  DBI::dbExecute(conn, glue::glue_sql(
    "UPDATE {`etl_schema`}.{`etl_table`}
    SET {`field`} = GETDATE()
    WHERE id = {etl_batch_id}",
    .con = conn))
}

#### QA SQL ROWS IN ETL LOG ####
qa_etl_rows_f <- function(
  conn,
  config,
  rows_sql) {
  
  ### SET DATABASE SETTINGS
  etl_schema <- "metadata"
  etl_table <- "pop_etl_log"
  
  for(i in 1:rows_sql) {
    DBI::dbExecute(conn, glue::glue_sql(
      "UPDATE {`etl_schema`}.{`etl_table`}
      SET qa_rows_load = {rows_sql[i,2]}
      WHERE id = {rows_sql[i,1]}",
      .con = conn))
  }
  
  sql_get <- glue::glue_sql(
    "SELECT id, qa_rows_file, qa_rows_load FROM {`etl_schema`}.{`etl_table`} 
      WHERE qa_rows_file <> qa_rows_load
      ORDER BY id ASC",
    .con = conn)
  qa_results <- DBI::dbGetQuery(conn, sql_get)
  return(qa_results)
}
