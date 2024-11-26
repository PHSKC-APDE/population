#### FUNCTIONS TO CREATE AND MODIFY ETL LOGS
#
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2020-12


#### FUNCTION CREATE ETL LOG ####
create_etl_log_f <- function(
  conn,
  server = NULL,
  config = NULL,
  etl_schema = NULL,
  etl_table = NULL,
  batch_name = NULL,
  batch_date = NULL,
  file_name = NULL,
  file_loc = NULL,
  geo_type =NULL,
  geo_scope = NULL,
  geo_year = NULL,
  census_year = NULL,
  year = NULL,
  r_type = NULL,
  rows_file = NULL,
  pop_file = NULL,
  df = NULL) {
  
  ### ERROR CHECKS ###
  if((is.null(server) || is.null(config)) & (is.null(etl_schema) || is.null(etl_table))) {
    stop("Either server and config OR etl_schema and etl_table must be specified.")
  }
  if(is.null(batch_name)) { 
    stop("batch_name must be specified.")
  }
  
  ### SETTING UP VARIABLES ###
  if(is.null(etl_schema)) { etl_schema <- config[[server]]$etl_schema }
  if(is.null(etl_table)) { etl_table <- config[[server]]$etl_table }
  if(is.null(etl_table)) { etl_table <- config$etl_table }
  if(is.null(batch_date)) {
    if(!is.na(as.numeric(substr(batch_name, 5, 6)))) { 
      batch_date <- paste0(substr(batch_name, 1, 4), "-",
                                substr(batch_name, 5, 6), "-",
                                substr(batch_name, 7, 8)) 
    } else { 
      batch_date <- paste0(substr(batch_name, 1, 4), "-01-01") 
    }
  }
  if(is.null(file_name)) {
    if(!is.null(df$file_name)) {
      file_name <- df$file_name
    } else {
      stop("file_name OR df must be specified.")
    }
  }
  if(is.null(file_loc)) {
    if(!is.null(df$file_loc)) {
      file_loc <- df$file_loc
    } else {
      stop("file_loc OR df must be specified.")
    }
  }
  if(is.null(geo_type)) {
    if(!is.null(df$geo_type)) {
      geo_type <- df$geo_type
    } else {
      stop("geo_type OR df must be specified.")
    }
  }
  if(is.null(geo_scope)) {
    if(!is.null(df$geo_scope)) {
      geo_scope <- df$geo_scope
    } else {
      stop("geo_scope OR df must be specified.")
    }
  }
  if(is.null(geo_year)) {
    if(!is.null(df$geo_year)) {
      geo_year <- df$geo_year
    } else {
      stop("geo_year OR df must be specified.")
    }
  }
  if(is.null(year)) {
    if(!is.null(df$year)) {
      year <- df$year
    } else {
      stop("year OR df must be specified.")
    }
  }
  if(is.null(census_year)) {
    if(!is.null(df$census_year)) {
      census_year <- df$census_year
    } else {
      stop("census_year OR df must be specified.")
    }
  }
  if(is.null(r_type)) {
    if(!is.null(df$r_type)) {
      r_type <- df$r_type
    } else {
      stop("r_type OR df must be specified.")
    }
  }
  if(is.null(rows_file)) {
    if(!is.null(df$rows_file)) {
      rows_file <- df$rows_file
    } else {
      stop("rows_file OR df must be specified.")
    }
  }
  if(is.null(pop_file)) {
    if(!is.null(df$pop_file)) {
      pop_file <- df$pop_file
    } else {
      stop("pop_file OR df must be specified.")
    }
  }
  
  ### CHECK IF ALREADY IN ETL LOG
  sql_get <- glue::glue_sql(
    "SELECT TOP (1) id FROM {`etl_schema`}.{`etl_table`} 
      WHERE batch_name = {batch_name} 
      AND file_name = {file_name}
      AND geo_type = {geo_type} AND ISNULL(geo_scope, 0) = ISNULL({geo_scope}, 0)
      AND geo_year = {geo_year} AND year = {year} AND r_type = {r_type}
      AND census_year = {census_year}
      ORDER BY id DESC",
    .con = conn)
  etl_batch_id <- DBI::dbGetQuery(conn, sql_get)
  
  if (nrow(etl_batch_id) == 0) {
    ### CREATE NEW ETL BATCH ID
    sql_load <- glue::glue_sql(
      "INSERT INTO {`etl_schema`}.{`etl_table`} 
        (batch_name, batch_date, file_name, file_loc, 
        geo_type, geo_scope, geo_year, census_year, year, r_type, 
        qa_rows_file, qa_pop_file, last_update_datetime) 
        VALUES ({batch_name}, {batch_date}, {file_name}, {file_loc}, 
        {geo_type}, {geo_scope}, {geo_year}, {census_year}, {year}, {r_type},
        {rows_file}, ROUND({pop_file}, 0), GETDATE())", 
      .con = conn)
    DBI::dbExecute(conn, sql_load)
  
    ### GET NEW ETL BATCH ID
    sql_get <- glue::glue_sql(
      "SELECT TOP (1) id FROM {`etl_schema`}.{`etl_table`} 
        WHERE batch_name = {batch_name} 
        AND file_name = {file_name}
        AND geo_type = {geo_type} AND ISNULL(geo_scope, 0) = ISNULL({geo_scope}, 0)
        AND geo_year = {geo_year} AND year = {year} AND r_type = {r_type}
        AND census_year = {census_year}
        ORDER BY id DESC",
      .con = conn)
    etl_batch_id <- DBI::dbGetQuery(conn, sql_get)
  } else {
    ### UPDATE PREVIOUS ETL BATCH ID ENTRY
    sql_load <- glue::glue_sql(
      "UPDATE {`etl_schema`}.{`etl_table`} 
        SET batch_name = {batch_name}, batch_date = {batch_date}, 
          file_name = {file_name}, file_loc = {file_loc}, 
          geo_type = {geo_type}, geo_scope = {geo_scope}, 
          geo_year = {geo_year}, year = {year}, r_type = {r_type}, 
          qa_rows_file = {rows_file}, qa_pop_file = {pop_file}, 
          census_year = {census_year}, 
          last_update_datetime = GETDATE()
        WHERE id = {as.numeric(etl_batch_id)}", 
      .con = conn)
    DBI::dbExecute(conn, sql_load)
  }
  return(as.numeric(etl_batch_id))
}

get_etl_list_to_load_f <- function(
  conn,
  server = NULL,
  config = NULL,
  etl_schema = NULL,
  etl_table = NULL,
  geo_types = c(),
  census_year = NULL,
  min_year = 0,
  base_path = NULL,
  base_url = NULL,
  batch_name = NULL,
  get_folders = F,
  sql_source = F,
  folders = c()) {

  ### ERROR CHECKS ###
  if((is.null(server) || is.null(config)) & (is.null(etl_schema) || is.null(etl_table))) {
    stop("Either server and config OR etl_schema and etl_table must be specified.")
  }
  if(is.null(base_path) & is.null(base_url)) { 
    stop("base_path AND/OR base_url must be specified.")
  }
  
  ### SETTING UP VARIABLES ###
  if(is.null(etl_schema)) { etl_schema <- config[[server]]$etl_schema }
  if(is.null(etl_table)) { etl_table <- config[[server]]$etl_table }
  if(is.null(etl_table)) { etl_table <- config$etl_table }

  if(is.null(batch_name)) {
    sql_list <- glue::glue_sql("SELECT batch_name 
                               FROM {`etl_schema`}.{`etl_table`} 
                               WHERE load_ref_datetime IS NULL 
                               AND year >= {min_year} 
                               AND census_year = {census_year} ", 
                               .con = conn)
    if(sql_source == T) {
      sql_list <- glue::glue_sql("{sql_list} 
                                 AND file_loc = 'ref' ", 
                                 .con = conn)
    } else {
      sql_list <- glue::glue_sql("{sql_list} 
                                 AND file_loc <> 'ref' ", 
                                 .con = conn)
    }
    if(length(geo_types) > 0) {
      sql_list <- glue::glue_sql("{sql_list} 
                                 AND geo_type IN({DBI::SQL(
                                  glue::glue_collapse(
                                    glue::glue_sql('{geo_types}', 
                                      .con = conn), 
                                    sep = ','))})", 
                                 .con = conn)
    }
    sql_list <- glue::glue_sql("{sql_list} 
                              GROUP BY batch_name, batch_date 
                              ORDER BY batch_date DESC",
                              .con = conn)
  } else if(get_folders == T) {
    sql_list <- glue::glue_sql("SELECT 
                                REPLACE(
                                  REPLACE(file_loc, {base_url}, ''), 
                                  {paste0(base_path, '/')}, '')
                               FROM {`etl_schema`}.{`etl_table`} 
                               WHERE load_ref_datetime IS NULL
                               AND batch_name = {batch_name}
                               AND year >= {min_year}
                               AND census_year = {census_year} ", 
                               .con = conn)
    if(length(geo_types) > 0) {
      sql_list <- glue::glue_sql("{sql_list} 
                                 AND geo_type IN({DBI::SQL(
                                  glue::glue_collapse(
                                    glue::glue_sql('{geo_types}', 
                                      .con = conn), 
                                    sep = ','))})", 
                                 .con = conn)
    }
    sql_list <- glue::glue_sql("{sql_list} 
                              GROUP BY file_loc 
                               ORDER BY file_loc",
                               .con = conn)
  } else {
    sql_list <- glue::glue_sql("SELECT [id], [batch_name], [batch_date], 
                                [geo_type], [geo_scope], [geo_year], [year],
                                [r_type], [qa_rows_file], [qa_pop_file], 
                                [file_name], [file_loc] 
                               FROM {`etl_schema`}.{`etl_table`} 
                               WHERE load_ref_datetime IS NULL 
                               AND batch_name = {batch_name}
                               AND year >= {min_year}
                               AND census_year = {census_year} ", 
                               .con = conn)
    if(length(geo_types) > 0) {
      sql_list <- glue::glue_sql("{sql_list} 
                                 AND geo_type IN({DBI::SQL(
                                  glue::glue_collapse(
                                    glue::glue_sql('{geo_types}', 
                                      .con = conn), 
                                    sep = ','))})", 
                                 .con = conn)
    }
    if(length(folders) > 0) {
      sql_list <- glue::glue_sql("{sql_list} 
                                 AND (CHARINDEX({DBI::SQL(
                                  glue::glue_collapse(
                                    glue::glue_sql('{folders}', 
                                      .con = conn), 
                                    sep = ', file_loc) > 0 OR CHARINDEX('))}, file_loc) > 0)", 
                                 .con = conn)
    }
    sql_list <- glue::glue_sql("{sql_list} 
                               ORDER BY id",
                               .con = conn)
  }
  results <- DBI::dbGetQuery(conn, sql_list)
  return(results)
}

get_etl_log_f <- function(
  conn,
  etl_schema,
  etl_table,
  etl_batch_id = NULL) {
 
  if (is.null(etl_batch_id)) {
    results <- dbGetQuery(conn,
                          glue::glue_sql("SELECT id, batch_name, 
                                         batch_date, geo_type, geo_scope, 
                                         geo_year, census_year, year, r_type
                                         FROM {`etl_schema`}.{`etl_table`}
                                         ORDER BY id",
                                         .con = conn))
  } else {
    results <- dbGetQuery(conn,
                          glue::glue_sql("SELECT TOP (1) id, batch_name, 
                                         batch_date, geo_type, geo_scope, 
                                         geo_year, census_year, year, r_type
                                         FROM {`etl_schema`}.{`etl_table`}
                                         WHERE id = {etl_batch_id}",
                                         .con = conn))
  }
  return(results)
}

xget_etl_log_f <- function(
  conn,
  etl_schema,
  etl_table,
  etl_batch_id) {

  ### CHECK FOR FAILED LOAD RAW ###
  ### Returns the ID if this file has already been at least partially loaded into raw
  ###   but not made it to ref yet
  sql_get <- glue::glue_sql(
    "SELECT TOP (1) id FROM {`etl_schema`}.{`etl_table`} 
      WHERE batch_name = {batch_name} 
      AND file_name = {file_name} AND file_loc = {file_loc} 
      AND geo_type = {geo_type} AND ISNULL(geo_scope, 0) = ISNULL({geo_scope}, 0)
      AND geo_year = {geo_year} AND year = {year} AND r_type = {r_type}
      AND census_year = {census_year}
      AND load_ref_datetime IS NULL
      ORDER BY id DESC",
    .con = conn)
  etl_batch_id <- DBI::dbGetQuery(conn, sql_get)
  
  ### CHECK IF RAW DATA ALREADY LOADED TO REF ###
  ### Returns a negative ID if this batch and file has already been loaded through ref
  if (nrow(etl_batch_id) == 0) {
    sql_get <- glue::glue_sql(
      "SELECT TOP (1) (id * -1) FROM {`etl_schema`}.{`etl_table`} 
      WHERE batch_name = {batch_name} 
      AND file_name = {file_name} AND file_loc = {file_loc} 
      AND geo_type = {geo_type} AND ISNULL(geo_scope, 0) = ISNULL({geo_scope}, 0)
      AND geo_year = {geo_year} AND year = {year} AND r_type = {r_type}
      AND load_ref_datetime IS NOT NULL
      AND census_year = {census_year}
      ORDER BY id DESC",
      .con = conn)
    etl_batch_id <- DBI::dbGetQuery(conn, sql_get)
  }
  
  ### CHECK IF RAW DATA HAS ALREADY BEEN LOADED TO ARCHIVE ###
  ### Returns a -999 ID if this batch and file has already been loaded through archive
  if (nrow(etl_batch_id) == 0) {
    sql_get <- glue::glue_sql(
      "SELECT TOP (1) -9999 FROM {`etl_schema`}.{`etl_table`}
      WHERE [load_archive_datetime] IS NOT NULL 
        AND [delete_archive_datetime] IS NULL
        AND [geo_type] = {geo_type} AND [geo_scope] = {geo_scope} 
        AND [geo_year] = {geo_year} AND [year] = {year} AND [r_type] = {r_type}
        AND census_year = {census_year}
      GROUP BY [geo_type], [geo_scope], [geo_year], [year], [r_type]
      HAVING COUNT(id) >= 2 AND MIN([batch_date]) > {batch_date}",
      .con = conn)
    etl_batch_id <- DBI::dbGetQuery(conn, sql_get)
  }
  
  ### CHECK IF NO ID EXISTS AND DIRECTED TO CREATE ONE
  if (nrow(etl_batch_id) == 0 & create_id == T) {
    ### CREATE NEW ETL BATCH ID
    sql_load <- glue::glue_sql(
      "INSERT INTO {`etl_schema`}.{`etl_table`} 
      (batch_name, batch_date, file_name, file_loc, geo_type, geo_scope, geo_year, census_year, year, r_type) 
      VALUES ({batch_name}, {batch_date}, {file_name}, {file_loc}, {geo_type}, 
      {geo_scope}, {geo_year}, {census_year}, {year}, {r_type})", 
      .con = conn)
    DBI::dbGetQuery(conn, sql_load)
  
    ### GET NEW ETL BATCH ID
    sql_get <- glue::glue_sql(
      "SELECT TOP (1) id FROM {`etl_schema`}.{`etl_table`} 
      WHERE batch_name = {batch_name} 
      AND file_name = {file_name} AND file_loc = {file_loc} 
      AND geo_type = {geo_type} AND ISNULL(geo_scope, 0) = ISNULL({geo_scope}, 0)
      AND geo_year = {geo_year} AND year = {year} AND r_type = {r_type}
      AND census_year = {census_year}
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
  etl_schema,
  etl_table,
  field) {

  DBI::dbExecute(conn, glue::glue_sql(
    "UPDATE {`etl_schema`}.{`etl_table`}
    SET {`field`} = GETDATE(), last_update_datetime = GETDATE()
    WHERE id = {etl_batch_id}",
    .con = conn))
}

#### QA SQL ROWS/POP IN ETL LOG ####
qa_etl_f <- function(
  conn,
  etl_batch_id,
  etl_schema,
  etl_table,
  qa_val = 0,
  field = 0,
  results = F) {
  
  if(field != 0) {
    DBI::dbExecute(conn, glue::glue_sql(
    "UPDATE {`etl_schema`}.{`etl_table`} 
    SET {`field`} = {qa_val} 
    WHERE id = {etl_batch_id}", 
    .con = conn))
  }
  sql_get <- glue::glue_sql(
    "SELECT id, qa_rows_file, qa_rows_raw, qa_rows_stage, qa_rows_ref,
      qa_pop_file, qa_pop_raw, qa_pop_stage, qa_pop_ref
      FROM {`etl_schema`}.{`etl_table`} 
      WHERE id = {etl_batch_id}",
    .con = conn)
  qa_results <- DBI::dbGetQuery(conn, sql_get)
  if(results == T) {
    return(qa_results)
  }
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
  etl_schema <- config$etl_schema
  etl_table <- config$etl_table

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
  