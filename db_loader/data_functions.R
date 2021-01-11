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
  zipped_files <- as.data.frame(list.files(paste0(path_raw, "/", f_load, "/files_to_load"), pattern = "\\.zip$", ignore.case = T))
  colnames(zipped_files)[1] = "filename"
  zipped_files <- as.data.frame(zipped_files[grepl("csv", zipped_files$filename, ignore.case = T), ])
  stime <- Sys.time()
  ### Begin loop to extract data to tmp folder, archive old sql tables, 
  ### prep new sql tables, insert data to sql, and remove tmp data - one zip file at a time
  message(paste0(f_load, ": Loading data from folder (", Sys.time() , ")"))
  for (z in 1:nrow(zipped_files)) {
    ### Clean out temp folder
    file.remove(list.files(path_tmp, include.dirs = F, full.names = T, recursive = T))
    ### Get list of files in the zip file
    message(paste0(f_load, " - ", zipped_files[z,], ": Reviewing files in zip"))
    files_in_zip <- utils::unzip(paste0(path_raw, "/", f_load, "/files_to_load/", zipped_files[z,]), list = TRUE)
    files_to_unzip <- c()
    ### Make a list of files that have not already been loaded for this batch
    for(u in 1:nrow(files_in_zip)) {
      file_info <- get_raw_file_info_f(config = pop_config, file_name = files_in_zip[u,]$Name)
      etl_batch_id <- create_etl_log_f(conn = conn, config = pop_config, 
                                       batch_name = f_load, file_name = files_in_zip[u,]$Name, 
                                       geo_type = file_info$geo_type, geo_scope = file_info$geo_scope,
                                       geo_year = file_info$geo_year, year = file_info$year, 
                                       r_type = file_info$r_type)
      if (etl_batch_id > 0) {
        files_to_unzip <- c(files_to_unzip, files_in_zip[u,]$Name)
      }
    }
    ### Run if there are files to process
    if (length(files_to_unzip) > 0) {
      message(paste0(f_load, " - ", zipped_files[z,], ": Unzipping files (", Sys.time() , ")"))
      # Unzip zip file to the tmp folder
      z_args <- c(glue(' e "{paste0(path_raw, "/", f_load, "/files_to_load/", zipped_files[z,])}"', 
                       ' -o"{path_tmp}"', 
                       ' -y',
                       ' "{glue_collapse(files_to_unzip, sep = \'" "\')}"'))
      system2(command = "7z", args = c(z_args))
      # Get a list of the unzipped files
      unzipped_files <- as.data.frame(list.files(path_tmp, pattern = "\\.csv$", ignore.case = T))
      # Look at one unzipped file at a time
      for (y in 1:nrow(unzipped_files)) {
        # Use file name to determine other elements of the data and add to data frame
        message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], ": Reviewing file name (", Sys.time() , ")"))
        file_info <- get_raw_file_info_f(config = pop_config, file_name = unzipped_files[y,])
        etl_batch_id <- create_etl_log_f(conn = conn, config = pop_config, 
                                        batch_name = f_load, file_name = unzipped_files[y,], 
                                        geo_type = file_info$geo_type, geo_scope = file_info$geo_scope,
                                        geo_year = file_info$geo_year, year = file_info$year, 
                                        r_type = file_info$r_type)
        ###
        in_geo_types <- c("zip")
        ex_geo_types <- c("blkg", "lgd", "ste", "trc")
        if (file_info$geo_type %in% in_geo_types == FALSE) { next }
        ###
        if (etl_batch_id < 0) {
          message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id * -1, ": Already loaded to ref.pop (", Sys.time() , ")"))
        }
        else {
          msg <- paste0("ETL Batch ID (", Sys.time() , ")")
          etl_log_notes_f(conn = conn, etl_batch_id = etl_batch_id, note = msg)
          message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id, ": ", msg))
          # Read csv into a data frame
          msg <- paste0("Reading data from file (", Sys.time() , ")")
          etl_log_notes_f(conn = conn, etl_batch_id = etl_batch_id, note = msg)
          message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id, ": ", msg))
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
          ### Record number of rows in file
          qa_etl_rows_f(conn = conn, config = pop_config, 
                        rows_sql = data.frame(c(etl_batch_id), c(nrow(data))),
                        "qa_rows_file")
          ### Add ETL Batch ID to data and reorder columns
          data["etl_batch_id"] = etl_batch_id
          data <- data[, c("etl_batch_id", "year", "geo_id", "racemars", 
                        "gender", "agestr", "hispanic", "pop")]
          ### Check if data has tried to load and how many rows were loaded
          data_start <- failed_raw_load_f(conn = conn, config = raw_config, 
                                        etl_batch_id = etl_batch_id)
          ### Remove rows from dataframe that have already been loaded to raw
          if(nrow(data_start) > 0) {
            data_start <- data_start[1,2]
            msg <- paste0("Loading data into raw that previously failed. Picking up at row ", data_start + 1, " (", Sys.time() , ")")
            etl_log_notes_f(conn = conn, etl_batch_id = etl_batch_id, note = msg)
            message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id, ": ", msg))
            if (data_start > 0) {
              data <- data[-(1:data_start),]
              retry <- T
            }
            else {
              retry <- F
            }
          }
          else {
            data_start = 0
            retry <- F
          }
          ### Load raw data to sql
          msg <- paste0("Loading raw data (", Sys.time() , ")")
          etl_log_notes_f(conn = conn, etl_batch_id = etl_batch_id, note = msg)
          message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id, ": ", msg))
          qa_rows_sql <- load_raw_f(conn = conn, config = raw_config, 
                                  path_tmp = path_tmp, path_tmptxt = path_tmptxt,
                                  data = data, etl_batch_id = etl_batch_id,
                                  retry = retry, write_local = T)
          ### Record number of rows loaded to raw
          qa_rows_results <- qa_etl_rows_f(conn = conn, config = pop_config,
                                        rows_sql = qa_rows_sql + data_start, "qa_rows_load")
          msg <- paste0("Raw data loaded (", Sys.time() , ")")
          etl_log_notes_f(conn = conn, etl_batch_id = etl_batch_id, note = msg)
          message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id, ": ", msg))
          ### Clean raw data and add columns
          msg <- paste0("Cleaning raw data (", Sys.time() , ")")
          etl_log_notes_f(conn = conn, etl_batch_id = etl_batch_id, note = msg)
          message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id, ": ", msg))
          clean_raw_f(conn = conn, config = raw_config, etl_batch_id = etl_batch_id)
          msg <- paste0("Raw data cleaned (", Sys.time() , ")")
          etl_log_notes_f(conn = conn, etl_batch_id = etl_batch_id, note = msg)
          message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id, ": ", msg))
          ### Determine if there is old data in ref that needs to be archived
          to_archive <- DBI::dbGetQuery(conn, glue::glue_sql(
            "SELECT A.id AS etl_batch_id
            FROM metadata.pop_etl_log A
            INNER JOIN [PH_APDEStore].[metadata].[pop_etl_log] R ON
  	          R.geo_type = A.geo_type AND ISNULL(R.geo_scope, 0) = ISNULL(A.geo_scope, 0)
	            AND R.geo_year = A.geo_year AND R.year = A.year AND R.r_type = A.r_type
            WHERE R.id = {etl_batch_id} AND A.id <> R.id AND A.load_archive_datetime IS NULL",
            .con = conn))
          ### Archive the old data and remove from ref
          if (nrow(to_archive) > 0) {
            for (a in 1:nrow(to_archive)) {
              msg <- paste0("Begin archiving old data from ETL Batch ID ", to_archive[a,1], " (", Sys.time() , ")")
              etl_log_notes_f(conn = conn, etl_batch_id = etl_batch_id, note = msg)
              message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id, ": ", msg))
              msg <- paste0("Moving old data from ref to archive for ETL Batch ID ", etl_batch_id, " (", Sys.time() , ")")
              etl_log_notes_f(conn = conn, etl_batch_id = to_archive[a,1], note = msg)
              message(paste0("ETL Batch ID - ", to_archive[a,1], ": ", msg))
              data_move_f(conn, pop_config$archive_schema, pop_config$ref_schema, pop_config$table_name, to_archive[a,1], T)
              update_etl_log_datetime_f(
                conn = conn, 
                etl_batch_id = to_archive[a, 1],
                field = "load_archive_datetime")
              msg <- paste0("Old data loaded to archive (", Sys.time() , ")")
              etl_log_notes_f(conn = conn, etl_batch_id = to_archive[a,1], note = msg)
              message(paste0("ETL Batch ID - ", to_archive[a,1], ": ", msg))
              update_etl_log_datetime_f(
                conn = conn, 
                etl_batch_id = to_archive[a, 1],
                field = "delete_ref_datetime")
              msg <- paste0("Old data deleted from ref (", Sys.time() , ")")
              etl_log_notes_f(conn = conn, etl_batch_id = to_archive[a,1], note = msg)
              message(paste0("ETL Batch ID - ", to_archive[a,1], ": ", msg))
              msg <- paste0("Archiving old data from ETL Batch ID ", to_archive[a,1], " complete (", Sys.time() , ")")
              etl_log_notes_f(conn = conn, etl_batch_id = etl_batch_id, note = msg)
              message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id, ": ", msg))
            }
          }
          ### Move data from raw to ref
          msg <- paste0("Moving new data from raw to ref (", Sys.time() , ")")
          etl_log_notes_f(conn = conn, etl_batch_id = etl_batch_id, note = msg)
          message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id, ": ", msg))
          data_move_f(conn, pop_config$ref_schema, pop_config$raw_schema, pop_config$table_name, etl_batch_id)
          update_etl_log_datetime_f(
            conn = conn, 
            etl_batch_id = etl_batch_id,
            field = "load_ref_datetime")
          msg <- paste0("New data loaded to ref (", Sys.time() , ")")
          etl_log_notes_f(conn = conn, etl_batch_id = etl_batch_id, note = msg)
          message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id, ": ", msg))
          update_etl_log_datetime_f(
            conn = conn, 
            etl_batch_id = etl_batch_id,
            field = "delete_raw_datetime")
          msg <- paste0("Raw data deleted (", Sys.time() , ")")
          etl_log_notes_f(conn = conn, etl_batch_id = etl_batch_id, note = msg)
          message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id, ": ", msg))
          ### File has been processed
          msg <- paste0("Data has been processed (", Sys.time() , ")")
          etl_log_notes_f(conn = conn, etl_batch_id = etl_batch_id, note = msg)
          message(paste0(f_load, " - ", zipped_files[z,], " - ", unzipped_files[y,], " - ", etl_batch_id, ": ", msg))
        }
      }
      ### Empty tmp folder of csv files
      file.remove(list.files(path_tmp, include.dirs = F, full.names = T, recursive = T))
      message(paste0(f_load, " - ", zipped_files[z,], ": Data has been processed (", Sys.time() , ")"))
    } else {
      message(paste0(f_load, " - ", zipped_files[z,], ": All data has been previously processed (", Sys.time() , ")"))
    }
  }
  etime <- Sys.time()
  message(paste0(f_load, ": Batch load complete - ", etime - stime,  " (", Sys.time() , ")"))
  ### Return TRUE if process completed fully, else the function will loop and try again
  return(T)
}

### FUNCTION TO MOVE DATA FROM ONE TABLE TO ANOTHER
data_move_f <- function(
  conn,
  to_schema,
  from_schema,
  table_name,
  etl_batch_id,
  include_id = F) {
  ### Removes data from the to table
#  DBI::dbExecute(conn, glue_sql("DELETE FROM {`to_schema`}.{`table_name`} 
#           WHERE etl_batch_id = {etl_batch_id}", .con = conn))
  ### Gets list of columns from to table
  cols <- get_table_cols_f(conn = conn, schema = to_schema, table = table_name)
  ### Exclude id column if include_id is false
  if (include_id == F) {
    cols <- cols[cols$col != "id",]
  }
  else {
    cols <- cols$col
  }
#  DBI::dbExecute(conn, glue::glue_sql(
#    "DECLARE @batch INT = 10000;
#    WHILE @batch > 0
#    BEGIN
#    BEGIN TRANSACTION
#      DELETE TOP (@batch) FROM {`from_schema`}.{`table_name`} 
#	      OUTPUT deleted.*
#	      INTO {`to_schema`}.{`table_name`}
#	    WHERE etl_batch_id = {etl_batch_id}
#      SET @batch = @@ROWCOUNT
#    COMMIT TRANSACTION
#    END", .con = conn))
  
  ### Insert data from the from table into the to table
  DBI::dbExecute(conn, glue::glue_sql(
    "INSERT INTO {`to_schema`}.{`table_name`} WITH (TABLOCK)
    ({DBI::SQL(glue::glue_collapse(glue::glue_sql('{`cols`}', .con = conn), sep = ', '))})
    SELECT {DBI::SQL(glue::glue_collapse(glue::glue_sql('{`cols`}', .con = conn), sep = ', '))}
    FROM {`from_schema`}.{`table_name`}
    WHERE etl_batch_id = {etl_batch_id}", .con = conn))
  ### Removes data from the from table
  DBI::dbExecute(conn, glue_sql("DELETE FROM {`from_schema`}.{`table_name`} 
           WHERE etl_batch_id = {etl_batch_id}", .con = conn))
  
}

### FUNCTION TO SELECT BATCH AND START THE LOAD PROCESS
select_process_data_f <- function(){
  # List of folders that have raw data
  f_list <- list.dirs(path = path_raw, full.names = F, recursive = F)
  f_list <- f_list[ f_list != "tmp"]
  ### CHOOSE THE FOLDER TO LOAD RAW DATA FROM ###
  message("CHOOSE THE FOLDER TO LOAD RAW DATA FROM")
  f_load <- select.list(choices = f_list)
  ### DROP INDEX FROM REF IF EXISTS
  #drop_index_f(conn = conn, schema = pop_config$ref_schema, 
  #             table = pop_config$table_name, index_name = pop_config$index_name)
  # Variables to know if the process finished or to try again when an error occurs
  trynum <- 1
  complete <- F
  ### Begin loop to load data. This will repeat if there was an error 
  ###   Example: SQL Connection Error
  repeat {
    message(paste0("Try #", trynum))
    complete <- tryCatch(load_data_f(conn = conn, pop_config = pop_config, 
                                     raw_config = raw_config, path_raw = path_raw, 
                                     path_tmp = path_tmp, path_tmptxt = path_tmptxt, 
                                     f_load = f_load), 
                         error = function(err) {
                           print(paste0("ERROR: ",err))
                         })
    trynum <- trynum + 1
    if (complete == T | trynum > 10) { break }
  }
  ### ADD INDEX TO REF
  #add_index_f(conn = conn, schema = pop_config$ref_schema, 
  #            table = pop_config$table_name, index_name = pop_config$index_name
}