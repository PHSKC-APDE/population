#### FUNCTIONS FOR DATA
#
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2020-12

#### FUNCTION LOAD RAW DATA TO SQL####
load_data_f <- function(
  conn,
  path_raw,
  path_tmp,
  path_tmptxt,
  f_load) {
  
  conn <- DBI::dbConnect(odbc::odbc(), "PH_APDEStore51")
  # Set path for 7-zip
  old_path <- Sys.getenv("PATH")
  Sys.setenv(PATH = paste(old_path, "C:\\ProgramData\\Microsoft\\AppV\\Client\\Integration\\562FBB69-6389-4697-9A54-9FF814E30039\\Root\\VFS\\ProgramFilesX64\\7-Zip", sep = ";"))
  ### Create list of zip files with csvs in raw data folder
  zipped_files <- as.data.frame(list.files(paste0(path_raw, "/", f_load, "/files_to_load"), pattern = "\\.zip$", ignore.case = T))
  colnames(zipped_files)[1] = "filename"
  zipped_files <- as.data.frame(zipped_files[grepl("csv", zipped_files$filename, ignore.case = T), ])
  stime <- Sys.time()
  ### Begin loop to extract data to tmp folder, archive old sql tables, 
  ### prep new sql tables, insert data to sql, and remove tmp data - one zip file at a time
  message(etl_log_notes_f(conn = conn, 
                          batch_name = f_load,
                          note = "Loading data from folder"))

  for (z in 1:nrow(zipped_files)) {
    ### Clean out temp folder
    file.remove(list.files(path_tmp, include.dirs = F, full.names = T, recursive = T))
    ### Get list of files in the zip file
    message(etl_log_notes_f(conn = conn, 
                            batch_name = f_load,
                            zip_name = zipped_files[z,],
                            note = "Reviewing files in zip"))
    files_in_zip <- utils::unzip(paste0(path_raw, "/", f_load, "/files_to_load/", zipped_files[z,]), list = TRUE)
    files_to_unzip <- c()
    ### Make a list of files that have not already been loaded for this batch
    if(!is.na(as.numeric(substr(f_load,5,6)))) { 
      f_load_date <- paste0(substr(f_load,1,4), "-",
                            substr(f_load,5,6), "-",
                            substr(f_load,7,8)) 
    } else { 
      f_load_date <- paste0(substr(f_load,1,4), "-01-01") 
    }
    for(u in 1:nrow(files_in_zip)) {
      file_info <- get_raw_file_info_f(config = config, file_name = files_in_zip[u,]$Name)
      if (file_info$geo_type %in% in_geo_types == FALSE | file_info$year < min_year) { next }
      etl_batch_id <- create_etl_log_f(conn = conn,
                                       batch_name = f_load, 
                                       batch_date = f_load_date, 
                                       file_name = files_in_zip[u,]$Name, 
                                       geo_type = file_info$geo_type, 
                                       geo_scope = file_info$geo_scope,
                                       geo_year = file_info$geo_year, 
                                       year = file_info$year, 
                                       r_type = file_info$r_type, create_id = F)
      if (etl_batch_id >= 0) {
        files_to_unzip <- c(files_to_unzip, files_in_zip[u,]$Name)
      }
    }
    ### Run if there are files to process
    if (length(files_to_unzip) > 0) {
      message(etl_log_notes_f(conn = conn, 
                              batch_name = f_load,
                              zip_name = zipped_files[z,],
                              note = "Unzipping files"))
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
        pop_config <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/population/master/config/common.pop.yaml"))
        # Use file name to determine other elements of the data and add to data frame
        message(etl_log_notes_f(conn = conn, 
                                batch_name = f_load,
                                zip_name = zipped_files[z,],
                                file_name = unzipped_files[y,],
                                note = "Reviewing file name"))
        file_info <- get_raw_file_info_f(config = pop_config, file_name = unzipped_files[y,])
        table_name = pop_config$table_name
        if(file_info$r_type == 77) { 
            table_name <- paste0(table_name, "77")
        }
        if (file_info$geo_type %in% in_geo_types == FALSE) { next }
        etl_batch_id <- create_etl_log_f(conn = conn,
                                         batch_name = f_load, 
                                         batch_date = f_load_date, 
                                         file_name = unzipped_files[y,], 
                                         geo_type = file_info$geo_type, 
                                         geo_scope = file_info$geo_scope,
                                         geo_year = file_info$geo_year, 
                                         year = file_info$year, 
                                         r_type = file_info$r_type)
        if (etl_batch_id < 0) {
          if (etl_batch_id == -9999) {
            message(etl_log_notes_f(conn = conn, 
                                    note = "Already have archived data",
                                    display_only = T))
          } else { message(etl_log_notes_f(conn = conn, 
                                  etl_batch_id = etl_batch_id * -1,
                                  note = paste0("Already loaded to ", pop_config$ref_schema, ".", table_name),
                                  display_only = T))
          }
        }
        else {
          message(etl_log_notes_f(conn = conn, 
                                  etl_batch_id = etl_batch_id,
                                  note = "ETL Batch ID"))
          qa <- qa_etl_f(conn = conn, etl_batch_id = etl_batch_id)
          if (qa$qa_rows_kept == qa$qa_rows_load & is.na(qa$qa_rows_load) == F) {
            message(etl_log_notes_f(conn = conn, 
                                    etl_batch_id = etl_batch_id,
                                    note = "Raw data already loaded"))
          } else {
            # Read csv into a data frame
            message(etl_log_notes_f(conn = conn, 
                                  etl_batch_id = etl_batch_id,
                                  note = "Reading data from file"))
            data <- read.csv(paste0(path_tmp, "/", unzipped_files[y,]))

            ### Record number of rows in file
            qa_etl_f(conn = conn, etl_batch_id = etl_batch_id,
                   qa_val = nrow(data), "qa_rows_file")
            
            ### Clean raw data
            message(etl_log_notes_f(conn = conn, 
                                    etl_batch_id = etl_batch_id,
                                    note = "Cleaning raw data"))
            clean_raw_r_f(conn = conn,
                          config = pop_config,
                          df = data,
                          info = file_info,
                          etl_batch_id = etl_batch_id)
            message(etl_log_notes_f(conn = conn, 
                                    etl_batch_id = etl_batch_id,
                                    note = "Raw data cleaned"))
            
            ### Check if raw data should go directly to archive
#            skip_ref <- raw_archive_f(conn = conn, 
#                                      archive_schema = pop_config$archive_schema,
#                                      raw_schema = pop_config$raw_schema,
#                                      table_name = table_name)
           
            schema_name <- pop_config$ref_schema
            ### Check if data has tried to load and how many rows were loaded
            data_start <- failed_raw_load_f(conn = conn, 
                                            schema_name = schema_name, 
                                            table_name = table_name,
                                            etl_batch_id = etl_batch_id)
            if(nrow(data) == data_start) {
              message(etl_log_notes_f(conn = conn, 
                                    etl_batch_id = etl_batch_id,
                                    note = "Raw data already loaded"))
            } else {
              if(data_start > 0 & is.na(qa$qa_rows_load) == T) {
                ### Remove rows from dataframe that have already been loaded to raw
                message(etl_log_notes_f(conn = conn, 
                                      etl_batch_id = etl_batch_id,
                                      note = paste0("Loading data that previously failed. Picking up at row ", data_start + 1)))
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
              message(etl_log_notes_f(conn = conn, 
                                    etl_batch_id = etl_batch_id,
                                    note = "Loading raw data"))
              qa_sql <- load_raw_f(conn = conn, 
                                   config = pop_config,
                                   schema_name = schema_name,
                                   table_name = table_name, 
                                   path_tmp = path_tmp, path_tmptxt = path_tmptxt,
                                   data = data, etl_batch_id = etl_batch_id,
                                   retry = retry, write_local = T)
              qa <- qa_etl_f(conn = conn, etl_batch_id = qa_sql[1,1],
                               qa_val = qa_sql[1,2], "qa_rows_load")
              
              ### Check if all data was loaded and try to load any data that did not load the first time
              if (qa$qa_rows_kept != qa$qa_rows_load) {
                ### Reset SQL Connections
                conn <- DBI::dbConnect(odbc::odbc(), "PH_APDEStore51")
                ### Remove rows from dataframe that have already been loaded to raw
                data_start <- failed_raw_load_f(conn = conn, 
                                                schema_name =  schema_name, 
                                                table_name = table_name,
                                                etl_batch_id = etl_batch_id)
                if (data_start > 0 & is.na(qa$qa_rows_load) == T) {
                  data <- data[-(1:data_start),]
                  retry <- T
                }
                else {
                  retry <- F
                }
                message(etl_log_notes_f(conn = conn, 
                                      etl_batch_id = etl_batch_id,
                                      note = paste0("Loading data that previously failed. Picking up at row ", data_start + 1)))
                message(etl_log_notes_f(conn = conn, 
                                      etl_batch_id = etl_batch_id,
                                      note = "Loading raw data "))
                qa_sql <- load_raw_f(conn = conn, 
                                     schema_name = schema_name,
                                     table_name = table_name,
                                     path_tmp = path_tmp, path_tmptxt = path_tmptxt,
                                     data = data, etl_batch_id = etl_batch_id,
                                     retry = retry, write_local = T)
              }
              ### Record number of rows and total pop loaded to raw
              qa_etl_f(conn = conn, etl_batch_id = qa_sql[1,1],
                      qa_val = qa_sql[1,2], "qa_rows_load")
              qa_etl_f(conn = conn, etl_batch_id = qa_sql[1,1],
                      qa_val = qa_sql[1,3], "qa_pop_load")
              message(etl_log_notes_f(conn = conn, 
                                    etl_batch_id = etl_batch_id,
                                    note = paste0("Data loaded to ", schema_name, ".", table_name)))
            }
          }
          
          ### Check for old data and move it from ref to archive
          load_archive_f(conn = conn, 
                         archive_schema = pop_config$archive_schema,
                         ref_schema = pop_config$ref_schema,
                         table_name = table_name)
          
          ### File has been processed
          message(etl_log_notes_f(conn = conn, 
                                  etl_batch_id = etl_batch_id,
                                  note = "Data has been processed"))
        }
      }
      ### Empty tmp folder of csv files
      file.remove(list.files(path_tmp, include.dirs = F, full.names = T, recursive = T))
      message(paste0(f_load, " - ", zipped_files[z,], ": Data has been processed (", Sys.time() , ")"))
      message(etl_log_notes_f(conn = conn, batch_name =  f_load,
                              zip_name = zipped_files[z,],
                              note = "Data has been processed"))
    } else {
      message(etl_log_notes_f(conn = conn, batch_name =  f_load,
                              zip_name = zipped_files[z,],
                              note = "All data has been previously processed"))
    }
  }
  etime <- Sys.time()
  message(etl_log_notes_f(conn = conn, batch_name = f_load,
                          note = paste0("Batch load complete - ", etime - stime)))
  ### Return TRUE if process completed fully, else the function will loop and try again
  return(T)
}

### FUNCTION TO MOVE DATA FROM ONE TABLE TO ANOTHER
data_move_f <- function(
  conn,
  to_schema,
  from_schema,
  table_name,
  etl_batch_id) {
  ### Removes data from the to table
  DBI::dbExecute(conn, glue_sql("DELETE FROM {`to_schema`}.{`table_name`} WITH (TABLOCK)
           WHERE etl_batch_id = {etl_batch_id}", .con = conn))
  ### Gets list of columns from to table
  cols <- get_table_cols_f(conn = conn, schema = to_schema, table = table_name)
  ### Exclude id column if include_id is false

  cols <- cols$col

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
  DBI::dbExecute(conn, glue_sql("DELETE FROM {`from_schema`}.{`table_name`} WITH (TABLOCK)
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
  #             table = table_name, index_name = pop_config$index_name)
  # Variables to know if the process finished or to try again when an error occurs
  trynum <- 1
  complete <- F
  ### Begin loop to load data. This will repeat if there was an error 
  ###   Example: SQL Connection Error
  repeat {
    message(paste0("Try #", trynum))
    complete <- tryCatch(load_data_f(conn = conn, 
                                     path_raw = path_raw, 
                                     path_tmp = path_tmp, 
                                     path_tmptxt = path_tmptxt, 
                                     f_load = f_load), 
                         error = function(err) {
                           print(paste0("ERROR: ",err))
                         })
    trynum <- trynum + 1
    if (complete == T | trynum > 2) { break }
  }
  ### ADD INDEX TO REF
  #add_index_f(conn = conn, schema = pop_config$ref_schema, 
  #            table = pop_config$table_name, index_name = pop_config$index_name
}