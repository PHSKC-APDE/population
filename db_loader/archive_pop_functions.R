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
  config) {
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
                        note = paste0("Moving old data from ref to archive for ETL Batch ID ", to_archive[a,2]),
                        full_note = F)))
      data_move_f(conn, config$archive_schema, config$ref_schema, config$table_name, to_archive[a,1], T)
      update_etl_log_datetime_f(
        conn = conn, 
        etl_batch_id = to_archive[a, 1],
        field = "load_archive_datetime")
      message(paste0("ETL Batch ID - ", to_archive[a,1], ": ",
        etl_log_notes_f(conn = conn, 
                        etl_batch_id = to_archive[a,1],
                        note = "Old data loaded to archive",
                        full_note = F)))
      update_etl_log_datetime_f(
        conn = conn, 
        etl_batch_id = to_archive[a, 1],
        field = "delete_ref_datetime")
      message(paste0("ETL Batch ID - ", to_archive[a,1], ": ",
        etl_log_notes_f(conn = conn, 
                        etl_batch_id = to_archive[a,1],
                        note = "Old data deleted from ref",
                        full_note = F)))
      message(
        etl_log_notes_f(conn = conn,
                        etl_batch_id = to_archive[a,2],
                        note = paste0("Archiving old data from ETL Batch ID ", to_archive[a,1], " complete")))
    }
  }
}
#### FUNCTION TO ARCHIVE OLD DATA ####
clean_archive_f <- function(
  conn,
  config,
  max_allowed) {
  
}