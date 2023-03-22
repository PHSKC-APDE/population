load_stage_f <- function(
  server,
  server_dw = NULL,
  prod = F,
  interactive_auth = F,
  raw_schema,
  stage_schema,
  raw_table,
  stage_table,
  etl_batch_id) {
  
  ### SETTING UP VARIABLES ###
  if(is.null(server)) { 
    stop("server must be specified.") 
  }
  if(is.null(server_dw)) {
    server_dw <- server
  }
  
  conn <- create_db_connection(server_dw, interactive = interactive_auth, prod = prod)
  if (DBI::dbExistsTable(conn, DBI::Id(schema = stage_schema, table = stage_table))) {
    ### Record stage data deletion ###
    del_id <- DBI::dbGetQuery(conn, 
                            glue::glue_sql("sELECT TOP (1) etl_batch_id 
                                           FROM {`stage_schema`}.{`stage_table`}", 
                                           .con = conn))
    conn_etl <- create_db_connection(server, interactive = interactive_auth, prod = prod)
    update_etl_log_datetime_f(conn = conn_etl, etl_batch_id = as.numeric(del_id),
                            etl_schema = ref_schema, etl_table = etl_table,
                            field = "delete_stage_datetime")
  } else {
    create_table(conn = conn,
                 server = server,
                 config = stageconfig,
                 to_schema = stage_schema,
                 to_table = stage_table,
                 overwrite = T)
  }
  
  ### Move data to stage ###
  data_move_f(conn = conn,
              from_schema = raw_schema,
              to_schema = stage_schema,
              from_table = raw_table,
              to_table = stage_table,
              overwrite = T,
              etl_batch_id = etl_batch_id,
              where = "(geo_scope = 'wa' OR fips_co IN(33, 53, 61))")
  DBI::dbDisconnect(conn)
  
  conn <- create_db_connection(server_dw, interactive = interactive_auth, prod = prod)
  qa_results <- get_row_pop_f(conn, stage_schema, stage_table, etl_batch_id)
  conn_etl <- create_db_connection(server, interactive = interactive_auth, prod = prod)
  update_etl_log_datetime_f(conn = conn_etl, etl_batch_id = etl_batch_id,
                            etl_schema = ref_schema, etl_table = etl_table,
                            field = "load_stage_datetime")
  qa_etl_f(conn = conn_etl, etl_batch_id = etl_batch_id,
           etl_schema = ref_schema, etl_table = etl_table,
           qa_val = qa_results$row_cnt, field = "qa_rows_stage")
  qa_etl_f(conn = conn_etl, etl_batch_id = etl_batch_id,
           etl_schema = ref_schema, etl_table = etl_table,
           qa_val = qa_results$pop_tot, field = "qa_pop_stage")
  DBI::dbDisconnect(conn)
  DBI::dbDisconnect(conn_etl)
}

clean_stage_f <- function(
  server,
  server_dw = NULL,
  prod = F,
  interactive_auth = F,
  schema_name,
  table_name,
  xwalk_schema,
  xwalk_table,
  hra_table,
  info,
  etl_batch_id) {
  
  ### SETTING UP VARIABLES ###
  if(is.null(server)) { 
    stop("server must be specified.") 
  }
  if(is.null(server_dw)) {
    server_dw <- server
  }
  if(server_dw == "APDEStore") { 
    tablock <- "WITH (TABLOCK) " 
    hra_schema <- xwalk_schema
  } else { 
    tablock <- "" 
    hra_schema <- schema_name
  }
  
  ### Get Crosswalk and Reorder Crosswalk so that rcode is set before r1r3 and r2r4
  #  and that r1r3 and r2r4 are set before using raw_hispanic to override
  conn_x <- create_db_connection(server, interactive = interactive_auth, prod = prod)
  crosswalk <- DBI::dbGetQuery(conn_x,
                glue::glue_sql("SELECT * FROM {`xwalk_schema`}.{`xwalk_table`} 
                               WHERE (r_type IS NULL OR r_type = {info$r_type}) ",
#                               ORDER BY new_column DESC, old_column DESC, 
#                                new_value_num, new_value_txt", 
                               .con = conn_x))
  hras <- DBI::dbGetQuery(conn_x,
                         glue::glue_sql("SELECT *
                                        FROM {`xwalk_schema`}.{`hra_table`}",
                                        .con = conn_x))
  #DBI::dbDisconnect(conn_x)
  crosswalk <- crosswalk %>% arrange(desc(new_column), desc(old_column), new_value_num, new_value_txt)
  
  ### SET AGE ###
  conn <- create_db_connection(server_dw, interactive = interactive_auth, prod = prod)
  DBI::dbExecute(conn,
                 glue::glue_sql("UPDATE {`schema_name`}.{`table_name`} {DBI::SQL(tablock)} 
                                SET age = SUBSTRING(raw_agestr, 1, 3)",
                                .con = conn))
  #DBI::dbDisconnect(conn)
  
  ### UPDATE COLUMNS BASED ON CROSSWALK ###
  for(y in 1:nrow(crosswalk)) {
    conn <- create_db_connection(server_dw, interactive = interactive_auth, prod = prod)
    z <- crosswalk[y, ]
    if(!is.na(z$new_value_num)) { 
      new_val <-  z$new_value_num  
    } else { 
      new_val <- z$new_value_txt 
    }
    sql <- glue::glue_sql("UPDATE {`schema_name`}.{`table_name`} {DBI::SQL(tablock)}
                          SET {`z$new_column`} = {new_val} 
                          WHERE ", .con = conn)
    if(!is.na(z$old_value_txt)) { 
      sql <- glue::glue_sql("{sql} {`z$old_column`}  = {z$old_value_txt}", 
                            .con = conn) 
    } else { 
      sql <- glue::glue_sql("{sql} {`z$old_column`} >= {z$old_value_num_min} 
                              AND {`z$old_column`} <= {z$old_value_num_max}", 
                            .con = conn) 
    }
    if (!is.na(z$r_type)) { 
      sql <- glue::glue_sql("{sql} AND \"r_type\" = {z$r_type}", .con = conn)
    }
    if (z$old_column != 'raw_hispanic') {
      sql <- glue::glue_sql("{sql} AND {`z$new_column`} IS NULL", .con = conn)
    }
    DBI::dbExecute(conn, sql)
    #DBI::dbDisconnect(conn)
  }
  
  ### SET race_ COLUMNS TO ZERO WHERE NULL ###
  conn <- create_db_connection(server_dw, interactive = interactive_auth, prod = prod)
  DBI::dbExecute(conn, glue::glue_sql("UPDATE {`schema_name`}.{`table_name`} {DBI::SQL(tablock)}
                                 SET race_wht = ISNULL(race_wht, 0), 
                                 race_blk = ISNULL(race_blk, 0), 
                                 race_aian = ISNULL(race_aian, 0), 
                                 race_as = ISNULL(race_as, 0), 
                                 race_nhpi = ISNULL(race_nhpi, 0), 
                                 race_hisp = ISNULL(race_hisp, 0)",
                                 .con = conn))
  #DBI::dbDisconnect(conn)
  
  ### SET HRA IDs ###
  if(info$geo_type == "blk") {
    conn <- create_db_connection(server_dw, interactive = interactive_auth, prod = prod)
    sql <- glue::glue_sql("UPDATE P {DBI::SQL(tablock)}
                            SET P.hra10 = X.hra_id
                            FROM {`schema_name`}.{`table_name`} P
                            INNER JOIN {`hra_schema`}.{`hra_table`} X 
                              ON P.geo_id = X.blk_geo_id
                            WHERE X.geo_year = {info$geo_year}
                              AND P.fips_co = 33", 
                          .con = conn)
#    DBI::dbExecute(conn, sql)
    DBI::dbDisconnect(conn)
  }
}

