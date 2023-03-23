#### FUNCTIONS TO DEAL WITH ARCHIVE DATA
#    - ADDING DATA TO THE ARCHIVE TABLE
#    - REMOVING DATA FROM THE ARCHIVE TABLE
#
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2021-01

#### FUNCTION TO ARCHIVE OLD DATA ####
load_archive_f <- function(
  server,
  config = NULL,
  prod = F,
  interactive_auth = F,
  archive_schema,
  archive_table,
  ref_schema,
  ref_table,
  etl_schema,
  etl_table) {
  
  conn_db <- create_db_connection(server, interactive = interactive_auth, prod = prod)
  
  ### Determine if there is old data in ref that needs to be archived
  to_archive <- DBI::dbGetQuery(conn_db, glue::glue_sql(
    "SELECT x.id AS 'archive_id', z.id AS 'ref_id', x.r_type
      FROM {`etl_schema`}.{`etl_table`} AS x
      INNER JOIN (SELECT geo_type, geo_year, census_year, r_type, year, 
          MAX(batch_date) AS max_batch_date, MIN(batch_date) AS min_batch_date
        FROM {`etl_schema`}.{`etl_table`}
        WHERE load_archive_datetime IS NULL AND load_ref_datetime IS NOT NULL
        GROUP BY geo_type, geo_scope, geo_year, census_year, r_type, year
        HAVING COUNT(id) > 1) AS y 
        ON x.geo_type = y.geo_type AND x.geo_year = y.geo_year 
          AND x.census_year = y.census_year AND x.r_type = y.r_type 
          AND x.year = y.year
      INNER JOIN {`etl_schema`}.{`etl_table`} AS z
        ON y.geo_type = z.geo_type AND y.geo_year = z.geo_year 
          AND x.census_year = z.census_year AND y.r_type = z.r_type 
          AND y.year = z.year 
          AND y.max_batch_date = z.batch_date
      WHERE x.batch_date = y.min_batch_date",
    .con = conn_db))
  DBI::dbDisconnect(conn_db)
  ### Archive the old data and remove from ref
  if (nrow(to_archive) > 0) {
    for (a in 1:nrow(to_archive)) {
      conn_db <- create_db_connection(server, interactive = interactive_auth, prod = prod)
      if (to_archive$r_type[a] == 77) { 
        from_table <- paste0(ref_table, "_77")
        to_table <- paste0(archive_table, "_77")
      } else { 
        from_table <- ref_table 
        to_table <- archive_table 
      }
      message(glue("ETL Batch ID - {to_archive$archive_id[a]}: Moving Data from Ref Table ({ref_schema}.{from_table}) to Archive Table ({archive_schema}.{to_table}) - {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}"))
      data_move_f(conn = conn_db,
                  from_schema = ref_schema,
                  to_schema = archive_schema,
                  from_table = from_table,
                  to_table = to_table,
                  del_to = T,
                  del_from = T,
                  etl_batch_id = to_archive[a,1])
      update_etl_log_datetime_f(conn = conn_db, 
                                etl_batch_id =  to_archive$archive_id[a],
                                etl_schema = ref_schema, 
                                etl_table = etl_table,
                                field = "load_archive_datetime")
      update_etl_log_datetime_f(conn = conn_db, 
                                etl_batch_id =  to_archive$archive_id[a],
                                etl_schema = ref_schema, 
                                etl_table = etl_table,
                                field = "delete_ref_datetime")
      DBI::dbDisconnect(conn_db)
    }
  }
  
}

#### FUNCTION TO SEND RAW DATA DIRECTLY TO ARCHIVE ####
raw_archive_f <- function(
  conn,
  etl_batch_id) {
  
  etl_schema <- config$etl_schema
  etl_table <- config$etl_table
  
  ### Determine if new data needs to go directly to archive
  to_archive <- DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT x.id AS 'archive_id', z.id AS 'ref_id', x.r_type
      FROM {`etl_schema`}.{`etl_table`} AS x
      INNER JOIN (SELECT geo_type, geo_year, census_year, r_type, year, 
          MAX(batch_date) AS max_batch_date
        FROM {`etl_schema`}.{`etl_table`}
        WHERE load_archive_datetime IS NULL AND load_ref_datetime IS NOT NULL
        GROUP BY geo_type, geo_scope, geo_year, r_type, year) AS y 
        ON x.geo_type = y.geo_type AND x.geo_year = y.geo_year 
          AND x.census_year = y.census_year AND x.r_type = y.r_type 
          AND x.year = y.year
      INNER JOIN {`etl_schema`}.{`etl_table`} AS z
        ON y.geo_type = z.geo_type AND y.geo_year = z.geo_year 
          AND y.census_year = z.census_year AND y.r_type = z.r_type 
          AND y.year = z.year AND y.max_batch_date = z.batch_date
      WHERE x.batch_date < y.max_batch_date 
		    AND x.id = {etl_batch_id}
		    AND x.load_ref_datetime IS NULL
		    AND x.load_archive_datetime IS NULL",
    .con = conn))
  
  return(to_archive)
}



#### FUNCTION TO DELETE OLD ARCHIVE DATA ####
clean_archive_f <- function(
  server,
  config = NULL,
  prod = F,
  interactive_auth = F,
  archive_schema,
  archive_table,
  etl_schema,
  etl_table,
  max_archive = 2) {
  
  if(server_dw == "APDEStore") { 
    tablock <- "WITH (TABLOCK) " 
  } else { 
    tablock <- "" 
  }
  
  conn_db <- create_db_connection(server, interactive = interactive_auth, prod = prod)
  
  to_delete <- DBI::dbGetQuery(conn_db, glue::glue_sql(
    "SELECT x.id, x.r_type
    FROM {`etl_schema`}.{`etl_table`} x
    INNER JOIN (SELECT geo_type, geo_scope, geo_year, census_year, year, r_type, 
        MIN(batch_date) AS 'min_batch'
      FROM {`etl_schema`}.{`etl_table`}
      WHERE load_archive_datetime IS NOT NULL AND delete_archive_datetime IS NULL
      GROUP BY geo_type, geo_scope, geo_year, year, r_type
      HAVING COUNT(id) > {max_archive}) y ON x.geo_type = y.geo_type 
        AND x.geo_scope = y.geo_scope AND x.geo_year = y.geo_year 
        AND x.year = y.year AND x.r_type = y.r_type A
        ND x.census_year = y.census_year AND x.batch_date = y.min_batch",
    .con = conn_db))
  DBI::dbDisconnect(conn_db)
  ### Archive the old data and remove from ref
  if (nrow(to_delete) > 0) {
    for (a in 1:nrow(to_delete)) {
      conn_db <- create_db_connection(server, interactive = interactive_auth, prod = prod)
      if (to_delete$r_type[a] == 77) { 
        del_table <- paste0(archive_table, "_77")
      } else { 
        del_table <- archive_table 
      }
      message(glue("ETL Batch ID - {to_delete$id[a]}: Deleting Old Data from Archive Table ({archive_schema}.{del_table}) - {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}"))
      DBI::dbExecute(conn_db, glue_sql("DELETE FROM {`archive_schema`}.{`del_table`} {DBI::SQL(tablock)}
           WHERE etl_batch_id = {to_delete$id[a]}", .con = conn_db))
      update_etl_log_datetime_f(conn = conn_db, 
                                etl_batch_id =  to_delete$id[a],
                                etl_schema = etl_schema, 
                                etl_table = etl_table,
                                field = "delete_archive_datetime")
      DBI::dbDisconnect(conn_db)
    }
  }
}
