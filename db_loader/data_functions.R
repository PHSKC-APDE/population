#### FUNCTIONS FOR DATA
#
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2020-12

#### FUNCTION LOAD RAW DATA TO SQL####
load_data_f <- function(
  conn,
  pop_config,
  raw_config,
  path_raw,
  path_tmp,
  path_tmptxt,
  f_load) {
  
  ### Create list of zip files with csvs in raw data folder
  zipped_files <- as.data.frame(list.files(paste0(path_raw, "/", f_load), pattern = "\\.zip$", ignore.case = T))
  colnames(zipped_files)[1] = "filename"
  zipped_files <- as.data.frame(zipped_files[grepl("csv", zipped_files$filename, ignore.case = T), ])
  
  stime <- Sys.time()
  ### Begin loop to extract data to tmp folder, archive old sql tables, 
  ### prep new sql tables, insert data to sql, and remove tmp data - one zip file at a time
  message(paste0(f_load, ": Loading data from folder"))
  for (z in 1:nrow(zipped_files)) {
    message(paste0(f_load, " - ", zipped_files[z,], ": Unzipping file"))
    data_load <- data.frame(matrix(ncol = 0, nrow = 0))
    # Unzip each file to the tmp folder one at a time
    unzip(paste0(path_raw, "/", f_load, "/", zipped_files[z,]), exdir = path_tmp)
    # Get a list of the unzipped files
    unzipped_files <- as.data.frame(list.files(path_tmp, pattern = "\\.csv$", ignore.case = T))
    # Look at one unzipped file at a time
    for (y in 1:nrow(unzipped_files)) {
      # Use file name to determine other elements of the data and add to data frame
      file_info <- get_raw_file_info_f(config = pop_config, file_name = unzipped_files[y,])
      etl_batch_id <- create_etl_log_f(conn = conn, config = pop_config, 
                                       batch_name = f_load, file_name = unzipped_files[y,], 
                                       geo_type = file_info$geo_type, geo_scope = file_info$geo_scope,
                                       geo_year = file_info$geo_year, year = file_info$year, 
                                       r_type = file_info$r_type)
      if (etl_batch_id < 0) {
        message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id * -1, ": Already loaded to ref.pop"))  
      }
      else {
        message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id, ": ETL Batch ID"))
        # Read csv into a data frame
        message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], ": Reading file"))
        data <- read.csv(paste0(path_tmp, "/", unzipped_files[y,]))
        # Change names of columns
        colnames(data) <- lapply(colnames(data), tolower)
        for (x in 1:length(colnames(data))) {
          if (grepl("pop", colnames(data)[x]) == T) { 
            colnames(data)[x] = "pop"
          } else if (grepl("racemars", colnames(data)[x]) == T) {
            colnames(data)[x] = "racemars"
          }
          else if (grepl("age", colnames(data)[x]) == T) {
            colnames(data)[x] = "agestr"
          }
          else if (grepl("code", colnames(data)[x]) == T) {
            colnames(data)[x] = "geo_id"
          }
        }
        qa_etl_rows_f(conn = conn, config = pop_config, 
                      rows_sql = data.frame(c(etl_batch_id), c(nrow(data))),
                      "qa_rows_file")
        
        data["etl_batch_id"] = etl_batch_id
        data <- data[, c("etl_batch_id", "year", "geo_id", "racemars", 
                       "gender", "agestr", "hispanic", "pop")]
      
        data_start <- failed_raw_load_f(conn = conn, config = raw_config, 
                                      etl_batch_id = etl_batch_id)
        if(nrow(data_start) > 0) {
          message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id, 
                         ": Loading data into raw.pop previously failed. Picking up at row ", data_start[1,2] + 1))
          data <- data[-(1:data_start[1,2]),]
        }
        message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id, ": Loading raw data"))
        qa_rows_sql <- load_raw_f(conn = conn, config = raw_config, 
                                path_tmp = path_tmp, path_tmptxt = path_tmptxt,
                                data = data, etl_batch_id = etl_batch_id)
        qa_rows_results <- qa_etl_rows_f(conn = conn, config = pop_config,
                                       rows_sql = qa_rows_sql, "qa_rows_load")
        message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id, ": Cleaning raw data"))
        clean_raw_f(conn = conn, config = raw_config)
        to_archive <- DBI::dbGetQuery(conn, glue::glue_sql(
          "SELECT A.id AS etl_batch_id
          FROM metadata.pop_etl_log A
          INNER JOIN [PH_APDEStore].[metadata].[pop_etl_log] R ON
	          R.geo_type = A.geo_type AND ISNULL(R.geo_scope, 0) = ISNULL(A.geo_scope, 0)
	          AND R.geo_year = A.geo_year AND R.year = A.year AND R.r_type = A.r_type
          WHERE R.id = {etl_batch_id} AND A.id <> R.id AND A.load_archive_datetime IS NULL",
          .con = conn))
        if (nrow(to_archive) > 0) {
          for (a in 1:nrow(to_archive)) {
            message(paste0("ETL Batch ID - ", to_archive[a,1], ": Moving old data from ref.pop to archive.pop"))
            data_move_f(conn, pop_config$archive_schema, pop_config$ref_schema, pop_config$table_name, to_archive[a,1], T)
            update_etl_log_datetime_f(
              conn = conn, 
              etl_batch_id = to_archive[a, 1],
              field = "load_archive_datetime")
            update_etl_log_datetime_f(
              conn = conn, 
              etl_batch_id = to_archive[a, 1],
              field = "delete_ref_datetime")
          }
        }
        message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id, ": Moving new from raw.pop to ref.pop"))
        data_move_f(conn, pop_config$ref_schema, pop_config$raw_schema, pop_config$table_name, etl_batch_id)
        update_etl_log_datetime_f(
          conn = conn, 
          etl_batch_id = etl_batch_id,
          field = "load_ref_datetime")
        update_etl_log_datetime_f(
          conn = conn, 
          etl_batch_id = etl_batch_id,
          field = "delete_raw_datetime")
        message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id, ": Data has been processed"))
      }
    }
    file.remove(list.files(path_tmp, include.dirs = F, full.names = T, recursive = T))
    message(paste0(f_load, " - ", zipped_files[z,], ": Data has been processed"))
  }
  etime <- Sys.time()
  message(paste0(f_load, ": Batch load complete - ", etime - stime))
}

data_move_f <- function(
  conn,
  to_schema,
  from_schema,
  table_name,
  etl_batch_id,
  include_id = F) {
  
  DBI::dbExecute(conn, glue_sql("DELETE FROM {`to_schema`}.{`table_name`} 
           WHERE etl_batch_id = {etl_batch_id}", .con = conn))
  
  cols <- get_table_cols_f(conn = conn, schema = to_schema, table = table_name)
  
  if (include_id == F) {
    cols <- cols[cols$col != "id",]
  }
  else {
    cols <- cols$col
  }
  
  DBI::dbExecute(conn, glue::glue_sql(
    "INSERT INTO {`to_schema`}.{`table_name`} 
    ({DBI::SQL(glue::glue_collapse(glue::glue_sql('{`cols`}', .con = conn), sep = ', '))})
    SELECT {DBI::SQL(glue::glue_collapse(glue::glue_sql('{`cols`}', .con = conn), sep = ', '))}
    FROM {`from_schema`}.{`table_name`}
    WHERE etl_batch_id = {etl_batch_id}", .con = conn))
  
  DBI::dbExecute(conn, glue_sql("DELETE FROM {`from_schema`}.{`table_name`} 
           WHERE etl_batch_id = {etl_batch_id}", .con = conn))
}
