#### FUNCTIONS TO DEAL WITH ARCHIVE DATA
#    - ADDING DATA TO THE ARCHIVE TABLE
#    - REMOVING DATA FROM THE ARCHIVE TABLE
#
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2021-01

#### FUNCTION TO ARCHIVE OLD DATA ####
load_archive_f <- function(
  conn,
  archive_schema,
  ref_schema,
  table_name) {
  ### Determine if there is old data in ref that needs to be archived
  to_archive <- DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT x.id AS 'archive_id', z.id AS 'ref_id'
      FROM [PH_APDEStore].[metadata].[pop_etl_log] AS x
      INNER JOIN (SELECT geo_type, geo_year, r_type, year, 
          MAX(batch_date) AS max_batch_date
        FROM [PH_APDEStore].[metadata].[pop_etl_log]
        WHERE load_archive_datetime IS NULL AND load_ref_datetime IS NOT NULL
        GROUP BY geo_type, geo_scope, geo_year, r_type, year
        HAVING COUNT(id) > 1) AS y 
        ON x.geo_type = y.geo_type AND x.geo_year = y.geo_year 
          AND x.r_type = y.r_type AND x.year = y.year
      INNER JOIN [PH_APDEStore].[metadata].[pop_etl_log] AS z
        ON y.geo_type = z.geo_type AND y.geo_year = z.geo_year 
          AND y.r_type = z.r_type AND y.year = z.year 
          AND y.max_batch_date = z.batch_date
      WHERE x.batch_date <> y.max_batch_date",
    .con = conn))
  ### Archive the old data and remove from ref
  if (nrow(to_archive) > 0) {
    for (a in 1:nrow(to_archive)) {
      message(
        etl_log_notes_f(conn = conn,
                      etl_batch_id = to_archive[a,2],
                      note = paste0("Begin archiving old data from ETL Batch ID ", to_archive[a,1])))
      message(paste0("ETL Batch ID - ", to_archive[a,1], ": ",
        etl_log_notes_f(conn = conn, 
                        etl_batch_id = to_archive[a,1],
                        note = paste0("Moving old data from ", ref_schema, ".", 
                                      table_name, " to ", archive_schema, ".", 
                                      table_name, " for ETL Batch ID ", 
                                      to_archive[a,2]),
                        full_msg = F)))
      data_move_f(conn = conn, to_schema = archive_schema, 
                  from_schema = ref_schema, table_name = table_name, 
                  etl_batch_id = to_archive[a,1])
      update_etl_log_datetime_f(
        conn = conn, 
        etl_batch_id = to_archive[a, 1],
        field = "load_archive_datetime")
      message(paste0("ETL Batch ID - ", to_archive[a,1], ": ",
        etl_log_notes_f(conn = conn, 
                        etl_batch_id = to_archive[a,1],
                        note = paste0("Old data loaded to ", archive_schema, ".", 
                                      table_name),
                        full_msg = F)))
      
      update_etl_log_datetime_f(
        conn = conn, 
        etl_batch_id = to_archive[a, 1],
        field = "delete_ref_datetime")
      message(paste0("ETL Batch ID - ", to_archive[a,1], ": ",
        etl_log_notes_f(conn = conn, 
                        etl_batch_id = to_archive[a,1],
                        note = paste0("Old data deleted from ", ref_schema, ".", 
                                      table_name),
                        full_msg = F)))
      message(
        etl_log_notes_f(conn = conn,
                        etl_batch_id = to_archive[a,2],
                        note = paste0("Archiving old data from ETL Batch ID ", 
                                      to_archive[a,1], " complete")))
    }
  }
  clean_archive_f(conn, archive_schema, table_name)
}

#### FUNCTION TO SEND RAW DATA DIRECTLY TO ARCHIVE ####
raw_archive_f <- function(
  conn,
  archive_schema,
  raw_schema,
  table_name) {
  ### Determine if there is old data in ref that needs to be archived
  to_archive <- DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT x.id AS 'archive_id', z.id AS 'ref_id', x.r_type
      FROM [metadata].[pop_etl_log] AS x
      INNER JOIN (SELECT geo_type, geo_year, r_type, year, 
          MAX(batch_date) AS max_batch_date
        FROM [metadata].[pop_etl_log]
        WHERE load_archive_datetime IS NULL AND load_ref_datetime IS NOT NULL
        GROUP BY geo_type, geo_scope, geo_year, r_type, year) AS y 
        ON x.geo_type = y.geo_type AND x.geo_year = y.geo_year 
          AND x.r_type = y.r_type AND x.year = y.year
      INNER JOIN [metadata].[pop_etl_log] AS z
        ON y.geo_type = z.geo_type AND y.geo_year = z.geo_year 
          AND y.r_type = z.r_type AND y.year = z.year 
          AND y.max_batch_date = z.batch_date
      WHERE x.batch_date < y.max_batch_date 
		    AND x.load_raw_datetime IS NOT NULL
		    AND x.load_ref_datetime IS NULL
		    AND x.load_archive_datetime IS NULL",
    .con = conn))
  ### Archive the old data and remove from ref
  if (nrow(to_archive) > 0) {
    for (a in 1:nrow(to_archive)) {
      message(
        etl_log_notes_f(conn = conn,
                        etl_batch_id = to_archive[a,2],
                        note = paste0("Begin archiving old data from ETL Batch ID ", 
                                      to_archive[a,1])))
      message(paste0("ETL Batch ID - ", to_archive[a,1], ": ",
                     etl_log_notes_f(conn = conn, 
                                     etl_batch_id = to_archive[a,1],
                                     note = paste0("Moving old data from ", 
                                                   raw_schema, ".", table_name, 
                                                   " to ", archive_schema, ".", 
                                                   table_name, 
                                                   " for ETL Batch ID ", 
                                                   to_archive[a,2]),
                                     full_msg = F)))
      data_move_f(conn = conn, to_schema = archive_schema, 
                  from_schema = raw_schema, table_name = table_name, 
                  etl_batch_id = to_archive[a,1])
      update_etl_log_datetime_f(
        conn = conn, 
        etl_batch_id = to_archive[a, 1],
        field = "delete_raw_datetime")
      update_etl_log_datetime_f(
        conn = conn, 
        etl_batch_id = to_archive[a, 1],
        field = "load_ref_datetime")
      update_etl_log_datetime_f(
        conn = conn, 
        etl_batch_id = to_archive[a, 1],
        field = "delete_ref_datetime")
      update_etl_log_datetime_f(
        conn = conn, 
        etl_batch_id = to_archive[a, 1],
        field = "load_archive_datetime")
      message(paste0("ETL Batch ID - ", to_archive[a,1], ": ",
                     etl_log_notes_f(conn = conn, 
                                     etl_batch_id = to_archive[a,1],
                                     note = paste0("Old data loaded to ", 
                                                   archive_schema, ".", 
                                                   table_name),
                                     full_msg = F)))
      message(paste0("ETL Batch ID - ", to_archive[a,1], ": ",
                     etl_log_notes_f(conn = conn, 
                                     etl_batch_id = to_archive[a,1],
                                     note = paste0("Old data deleted from ", 
                                                   raw_schema, ".", table_name),
                                     full_msg = F)))
      message(
        etl_log_notes_f(conn = conn,
                        etl_batch_id = to_archive[a,2],
                        note = paste0("Archiving old data from ETL Batch ID ", 
                                      to_archive[a,1], " complete")))
    }
    return(T)
    drop_table_f(conn = conn, schema = raw_schema, table = table_name)
  }
  return(F)
}



#### FUNCTION TO DELETE OLD ARCHIVE DATA ####
clean_archive_f <- function(
  conn,
  archive_schema,
  table_name) {
  to_delete <- DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT [id]
    FROM [metadata].[pop_etl_log] x
    INNER JOIN (SELECT [geo_type], [geo_scope], [geo_year], [year], [r_type], 
        MIN([batch_date]) AS 'min_batch'
      FROM [metadata].[pop_etl_log]
      WHERE [load_archive_datetime] IS NOT NULL AND [delete_archive_datetime] IS NULL
      GROUP BY [geo_type], [geo_scope], [geo_year], [year], [r_type]
      HAVING COUNT(id) > 2) y ON x.[geo_type] = y.[geo_type] 
        AND x.[geo_scope] = y.[geo_scope] AND x.[geo_year] = y.[geo_year] 
        AND x.[year] = y.[year] AND x.[r_type] = y.r_type 
        AND x.[batch_date] = y.min_batch",
    .con = conn))
  ### Archive the old data and remove from ref
  if (nrow(to_delete) > 0) {
    for (a in 1:nrow(to_delete)) {
      message(paste0("ETL Batch ID - ", to_delete[a,1], ": ",
                     etl_log_notes_f(conn = conn, 
                                     etl_batch_id = to_delete[a,1],
                                     note = paste0("Deleting old data from archive.", table_name),
                                     full_msg = F)))
      DBI::dbExecute(conn, glue_sql("DELETE FROM {`archive_schema`}.{`table_name`} WITH (TABLOCK)
           WHERE etl_batch_id = {to_delete[a,1]}", .con = conn))
      update_etl_log_datetime_f(
        conn = conn, 
        etl_batch_id = to_delete[a, 1],
        field = "delete_archive_datetime")
      message(paste0("ETL Batch ID - ", to_delete[a,1], ": ",
                     etl_log_notes_f(conn = conn, 
                                     etl_batch_id = to_delete[a,1],
                                     note = paste0("Old data deleted from archive.", table_name),
                                     full_msg = F)))
    }
    return(T)
  }
  return(F)
}