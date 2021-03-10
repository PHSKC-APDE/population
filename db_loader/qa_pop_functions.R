
### FUNCTION TO SELECT BATCH AND START THE QA PROCESS
select_qa_data_f <- function(){
  # List of folders that have raw data
  f_list <- list.dirs(path = path_raw, full.names = F, recursive = F)
  f_list <- f_list[ f_list != "tmp"]
  ### CHOOSE THE FOLDER TO QA RAW DATA FROM ###
  message("CHOOSE THE FOLDER TO QA RAW DATA FROM")
  f_load <- select.list(choices = f_list)
  
  # Set path for 7-zip
  old_path <- Sys.getenv("PATH")
  Sys.setenv(PATH = paste(old_path, "C:\\ProgramData\\Microsoft\\AppV\\Client\\Integration\\562FBB69-6389-4697-9A54-9FF814E30039\\Root\\VFS\\ProgramFilesX64\\7-Zip", sep = ";"))
  ### Create list of zip files with csvs in raw data folder
  zipped_files <- as.data.frame(list.files(paste0(path_raw, "/", f_load, "/files_to_load"), pattern = "\\.zip$", ignore.case = T))
  colnames(zipped_files)[1] = "filename"
  zipped_files <- as.data.frame(zipped_files[grepl("csv", zipped_files$filename, ignore.case = T), ])
  files <- data.frame(matrix(ncol = 6, nrow = 0))
  colnames(files) <- c("file_name", "geo_type", "geo_scope", "geo_year", "year", "r_type")
  
  
  for (z in 1:nrow(zipped_files)) {
    ### Clean out temp folder
    file.remove(list.files(path_tmp, include.dirs = F, full.names = T, recursive = T))
    ### Get list of files in the zip file
    z <- 9
    files_in_zip <- utils::unzip(paste0(path_raw, "/", f_load, "/files_to_load/", zipped_files[z,]), list = TRUE)
    files_to_unzip <- c()
    ### Make a list of files that have not already been loaded for this batch
    for(u in 1:nrow(files_in_zip)) {
      file_info <- get_raw_file_info_f(config = config, file_name = files_in_zip[u,]$Name)
      file_info$file_name <- files_in_zip[u,]$Name
      if (file_info$geo_type %in% in_geo_types == T & file_info$year >= min_year) { 
        files_to_unzip <- c(files_to_unzip, files_in_zip[u,]$Name)
        files <- rbind(files, file_info)
      }
    }
    ### Run if there are files to process
    if (length(files_to_unzip) > 0) {
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
        file_info <- get_raw_file_info_f(config = pop_config, file_name = unzipped_files[y,])
        table_name = pop_config$table_name
        if(file_info$r_type == 77) { 
          table_name <- paste0(table_name, "77")
        }
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
        data <- data[, c("year", "geo_id", "racemars", "gender", "agestr", "hispanic", "pop")]
      }
      ### Empty tmp folder of csv files
      #file.remove(list.files(path_tmp, include.dirs = F, full.names = T, recursive = T))
    }
  }
}

create_qa_pop_f <- function(conn){
  qa_config <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/population/master/config/qa.pop.yaml"))
  schema_name <- qa_config$schema_name
  qa_table <-  qa_config$table_name
  ref_table <- substring(qa_table, 1, 3)
  ref_table77 <- paste0(ref_table, "77")
  vars <- qa_config$vars
  sel_vars <- vars[1:5]
  cols <- qa_config$cols
  create_table_f(conn = conn, schema = schema_name, 
                 table = qa_table, vars = vars,
                 overwrite = T)
  for (c in 1:nrow(cols)) {
    insert_code <- glue::glue_sql(
      "INSERT INTO {`schema_name`}.{`qa_table`} 
      ({DBI::SQL(glue::glue_collapse(glue::glue_sql('{`names(vars)`}', 
      .con = conn), sep = ', '))}) 
      SELECT 
      {DBI::SQL(glue::glue_collapse(glue::glue_sql('{`names(sel_vars)`}', 
      .con = conn), sep = ', '))}, 
      {cols[c]}, {`cols[[c]]`}, SUM(\"pop\")
      FROM {`schema_name`}.{`ref_table`}
      GROUP BY
      {DBI::SQL(glue::glue_collapse(glue::glue_sql('{`names(sel_vars)`}', 
      .con = conn), sep = ', '))}, {`cols[[c]]`}
      ", .con = conn)
    DBI::dbExecute(conn, insert_code)
    insert_code <- glue::glue_sql(
      "INSERT INTO {`schema_name`}.{`qa_table`} 
      ({DBI::SQL(glue::glue_collapse(glue::glue_sql('{`names(vars)`}', 
      .con = conn), sep = ', '))}) 
      SELECT 
      {DBI::SQL(glue::glue_collapse(glue::glue_sql('{`names(sel_vars)`}', 
      .con = conn), sep = ', '))}, 
      {cols[c]}, {`cols[[c]]`}, SUM(\"pop\")
      FROM {`schema_name`}.{`ref_table77`}
      GROUP BY
      {DBI::SQL(glue::glue_collapse(glue::glue_sql('{`names(sel_vars)`}', 
      .con = conn), sep = ', '))}, {`cols[[c]]`}
      ", .con = conn)
    DBI::dbExecute(conn, insert_code)
  }
}
