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
  
  ### CHECK FOR FAILED LOAD RAW ###
  sql_get <- glue::glue_sql(
    "SELECT TOP (1) id FROM {`etl_schema`}.{`etl_table`} 
      WHERE batch_name = {batch_name} AND file_name = {file_name} 
      AND geo_type = {geo_type} AND ISNULL(geo_scope, 0) = ISNULL({geo_scope}, 0)
      AND geo_year = {geo_year} AND year = {year} AND r_type = {r_type}
      AND load_raw_datetime IS NULL
      ORDER BY id DESC",
    .con = conn)
  etl_batch_id <- DBI::dbGetQuery(conn, sql_get)
  
  ### CHECK IF RAW DATA ALREADY LOADED TO REF ###
  if (etl_batch_id == 0) {
    sql_get <- glue::glue_sql(
      "SELECT TOP (1) (id * -1) FROM {`etl_schema`}.{`etl_table`} 
      WHERE batch_name = {batch_name} AND file_name = {file_name} 
      AND geo_type = {geo_type} AND ISNULL(geo_scope, 0) = ISNULL({geo_scope}, 0)
      AND geo_year = {geo_year} AND year = {year} AND r_type = {r_type}
      AND load_raw_datetime IS NOT NULL AND load_ref_datetime IS NOT NULL
      ORDER BY id DESC",
      .con = conn)
    etl_batch_id <- DBI::dbGetQuery(conn, sql_get)
  }
  
  if (etl_batch_id == 0) {
    ### CREATE NEW ETL BATCH ID
    sql_load <- glue::glue_sql(
      "INSERT INTO {`etl_schema`}.{`etl_table`} 
      (batch_name, file_name, geo_type, geo_scope, geo_year, year, r_type, qa_rows_file) 
      VALUES ({batch_name}, {file_name}, {geo_type}, {geo_scope}, 
      {geo_year}, {year}, {r_type}, {qa_rows_file})", 
      .con = conn)
    DBI::dbGetQuery(conn, sql_load)
  
    ### GET NEW ETL BATCH ID
    sql_get <- glue::glue_sql(
      "SELECT TOP (1) id FROM {`etl_schema`}.{`etl_table`} 
      WHERE batch_name = {batch_name} AND file_name = {file_name} 
      AND geo_type = {geo_type} AND ISNULL(geo_scope, 0) = ISNULL({geo_scope}, 0)
      AND geo_year = {geo_year} AND year = {year} AND r_type = {r_type}
      AND load_raw_datetime IS NULL
      ORDER BY id DESC",
      .con = conn)
    etl_batch_id <- DBI::dbGetQuery(conn, sql_get)
  }
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
  rows_sql,
  field) {
  
  ### SET DATABASE SETTINGS
  etl_schema <- "metadata"
  etl_table <- "pop_etl_log"
  
  for(i in 1:nrow(rows_sql)) {
    DBI::dbExecute(conn, glue::glue_sql(
    "UPDATE {`etl_schema`}.{`etl_table`} 
    SET {`field`} = {rows_sql[i,2]} 
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
