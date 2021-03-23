#### FUNCTIONS TO CREATE AND MODIFY ETL LOGS
#
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2020-12


#### FUNCTION CREATE ETL LOG ####
create_etl_log_f <- function(
  conn,
  batch_name,
  batch_date,
  file_name,
  geo_type,
  geo_scope,
  geo_year,
  year,
  r_type,
  create_id = T) {
  
  ### SET DATABASE SETTINGS
  etl_schema <- "ref"
  etl_table <- "metadata_pop_etl_log"
  
  ### CHECK FOR FAILED LOAD RAW ###
  ### Returns the ID if this file has already been at least partially loaded into raw
  ###   but not made it to ref yet
  sql_get <- glue::glue_sql(
    "SELECT TOP (1) id FROM {`etl_schema`}.{`etl_table`} 
      WHERE batch_name = {batch_name} AND file_name = {file_name} 
      AND geo_type = {geo_type} AND ISNULL(geo_scope, 0) = ISNULL({geo_scope}, 0)
      AND geo_year = {geo_year} AND year = {year} AND r_type = {r_type}
      AND load_ref_datetime IS NULL
      ORDER BY id DESC",
    .con = conn)
  etl_batch_id <- DBI::dbGetQuery(conn, sql_get)
  
  ### CHECK IF RAW DATA ALREADY LOADED TO REF ###
  ### Returns a negative ID if this batch and file has already been loaded through ref
  if (nrow(etl_batch_id) == 0) {
    sql_get <- glue::glue_sql(
      "SELECT TOP (1) (id * -1) FROM {`etl_schema`}.{`etl_table`} 
      WHERE batch_name = {batch_name} AND file_name = {file_name} 
      AND geo_type = {geo_type} AND ISNULL(geo_scope, 0) = ISNULL({geo_scope}, 0)
      AND geo_year = {geo_year} AND year = {year} AND r_type = {r_type}
      AND load_ref_datetime IS NOT NULL
      ORDER BY id DESC",
      .con = conn)
    etl_batch_id <- DBI::dbGetQuery(conn, sql_get)
  }
  
  if (nrow(etl_batch_id) == 0) {
    sql_get <- glue::glue_sql(
      "SELECT TOP (1) -999 FROM {`etl_schema`}.{`etl_table`}
      WHERE [load_archive_datetime] IS NOT NULL 
        AND [delete_archive_datetime] IS NULL
        AND [geo_type] = {geo_type} AND [geo_scope] = {geo_scope} 
        AND [geo_year] = {geo_year} AND [year] = {year} AND [r_type] = {r_type}
      GROUP BY [geo_type], [geo_scope], [geo_year], [year], [r_type]
      HAVING COUNT(id) >= 2 AND MIN([batch_date]) > {batch_date}",
      .con = conn)
    etl_batch_id <- DBI::dbGetQuery(conn, sql_get)
  }
  
  if (nrow(etl_batch_id) == 0 & create_id == T) {
    ### CREATE NEW ETL BATCH ID IF NEEDED
    sql_load <- glue::glue_sql(
      "INSERT INTO {`etl_schema`}.{`etl_table`} 
      (batch_name, batch_date, file_name, geo_type, geo_scope, geo_year, year, r_type) 
      VALUES ({batch_name}, {batch_date}, {file_name}, {geo_type}, {geo_scope}, 
      {geo_year}, {year}, {r_type})", 
      .con = conn)
    DBI::dbGetQuery(conn, sql_load)
  
    ### GET NEW ETL BATCH ID
    sql_get <- glue::glue_sql(
      "SELECT TOP (1) id FROM {`etl_schema`}.{`etl_table`} 
      WHERE batch_name = {batch_name} AND file_name = {file_name} 
      AND geo_type = {geo_type} AND ISNULL(geo_scope, 0) = ISNULL({geo_scope}, 0)
      AND geo_year = {geo_year} AND year = {year} AND r_type = {r_type}
      AND load_ref_datetime IS NULL
      ORDER BY id DESC",
      .con = conn)
    etl_batch_id <- DBI::dbGetQuery(conn, sql_get)
  } else if(nrow(etl_batch_id) == 0 & create_id == F) { etl_batch_id <- 0 }
  return(as.numeric(etl_batch_id))
}

#### FUNCTION UPDATE ETL LOG DATETIME FIELD ####
update_etl_log_datetime_f <- function(
  conn,
  etl_batch_id,
  field) {

  ### SET DATABASE SETTINGS
  etl_schema <- "ref"
  etl_table <- "metadata_pop_etl_log"
  
  DBI::dbExecute(conn, glue::glue_sql(
    "UPDATE {`etl_schema`}.{`etl_table`}
    SET {`field`} = GETDATE()
    WHERE id = {etl_batch_id}",
    .con = conn))
}

#### QA SQL ROWS/POP IN ETL LOG ####
qa_etl_f <- function(
  conn,
  etl_batch_id,
  qa_val = 0,
  field = 0) {
  
  ### SET DATABASE SETTINGS
  etl_schema <- "ref"
  etl_table <- "metadata_pop_etl_log"
  
  if(field != 0) {
    DBI::dbExecute(conn, glue::glue_sql(
    "UPDATE {`etl_schema`}.{`etl_table`} 
    SET {`field`} = {qa_val} 
    WHERE id = {etl_batch_id}", 
    .con = conn))
  }
  sql_get <- glue::glue_sql(
    "SELECT id, qa_rows_file, qa_rows_kept, qa_rows_load, qa_pop_kept, qa_pop_load FROM {`etl_schema`}.{`etl_table`} 
      WHERE id = {etl_batch_id}",
    .con = conn)
  qa_results <- DBI::dbGetQuery(conn, sql_get)
  return(qa_results)
}

### APPENDS NOTE TO THE ETL_NOTES FIELD
etl_log_notes_f <- function(
  conn,
  note,
  etl_batch_id = 0,
  batch_name = 0,
  zip_name = 0,
  file_name = 0,
  full_msg = T,
  display_only = F) {
  
  ### SET DATABASE SETTINGS
  etl_schema <- "ref"
  etl_table <- "metadata_pop_etl_log"

  note <- paste0(note, " (", Sys.time(), ")")
  
  if (etl_batch_id > 0 & display_only == F) {
    DBI::dbExecute(conn, glue::glue_sql(
      "UPDATE {`etl_schema`}.{`etl_table`} 
        SET etl_notes = CONCAT(etl_notes, {note}, ';'), 
          last_update_datetime = GETDATE()
        WHERE id = {etl_batch_id}", 
      .con = conn))
  }
  msg <- ""
  if(full_msg == T) {
    if (etl_batch_id > 0) {
      e <- DBI::dbGetQuery(conn, glue::glue_sql(
        "SELECT id, batch_name, file_name 
        FROM {`etl_schema`}.{`etl_table`} 
        WHERE id = {etl_batch_id}",
          .con = conn))
      msg <- paste0(msg, 
                  e$batch_name, " - ",
                  e$file_name, " - ",
                  e$id, ": ")
    } else {
      if (batch_name != 0) { msg <- paste0(msg, batch_name) }
      if (zip_name != 0) { msg <- paste0(msg, " - ", zip_name) }
      if (file_name == 0) { msg <- paste0(msg, ": ")
      } else { msg <- paste0(msg, " - ", file_name, ": ") }
    }
  } 
  msg <- paste0(msg, note)
  ### This delay is to help order the notes in queries
  Sys.sleep(1)
  return(msg)
}
  