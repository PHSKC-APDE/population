
### FUNCTION TO SELECT BATCH AND START THE QA PROCESS
select_qa_data_f <- function(){
  
  qa_config <- yaml::yaml.load(httr::GET("https://raw.githubusercontent.com/PHSKC-APDE/population/master/config/qa.pop.yaml"))
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
  
  qa_ref <- DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT * FROM {`qa_config$schema_name`}.{`qa_config$table_name`}",
    .con = conn))
  qa_ref$ref_pop <- qa_ref$pop
  qa_ref <- select(qa_ref, -pop)
  qa <- data.frame(matrix(ncol = 8, nrow = 0))
  
  for (z in 1:nrow(zipped_files)) {
    ### Clean out temp folder
    file.remove(list.files(path_tmp, include.dirs = F, full.names = T, recursive = T))
    ### Get list of files in the zip file
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
        # Clean the data and get extra columns
        data <- clean_raw_r_f(conn = conn, 
                      df = read.csv(paste0(path_tmp, "/", unzipped_files[y,])), 
                      info = file_info, 
                      config = pop_config)

        # Add population totals for different groupings to qa dataframe
        qa <- rbind(qa, data %>%
                      group_by(geo_type, geo_scope, geo_year, r_type, year, col = "age", val = age) %>%
                      summarize_at(vars(pop), list(raw_pop = sum)))
        qa <- rbind(qa, data %>%
                      group_by(geo_type, geo_scope, geo_year, r_type, year, col = "age5", val = age5) %>%
                      summarize_at(vars(pop), list(raw_pop = sum)))
        qa <- rbind(qa, data %>%
                      group_by(geo_type, geo_scope, geo_year, r_type, year, col = "age11", val = age11) %>%
                      summarize_at(vars(pop), list(raw_pop = sum)))
        qa <- rbind(qa, data %>%
                      group_by(geo_type, geo_scope, geo_year, r_type, year, col = "age20", val = age20) %>%
                      summarize_at(vars(pop), list(raw_pop = sum)))
        qa <- rbind(qa, data %>%
                      group_by(geo_type, geo_scope, geo_year, r_type, year, col = "s", val = s) %>%
                      summarize_at(vars(pop), list(raw_pop = sum)))
        qa <- rbind(qa, data %>%
                      group_by(geo_type, geo_scope, geo_year, r_type, year, col = "r2_4", val = r2_4) %>%
                      summarize_at(vars(pop), list(raw_pop = sum)))
        qa <- rbind(qa, data %>%
                      group_by(geo_type, geo_scope, geo_year, r_type, year, col = "fips_co", val = fips_co) %>%
                      summarize_at(vars(pop), list(raw_pop = sum)))
        
      }
      ### Empty tmp folder of csv files
      file.remove(list.files(path_tmp, include.dirs = F, full.names = T, recursive = T))
    }
  }
  
  ### Create qa comparison columns and data sets
  qa_raw_v_cref <- as.data.frame(inner_join(qa, qa_ref))
  qa_raw_v_cref$diff <- with(qa_raw_v_cref, round(raw_pop - ref_pop, 6))
  qa_raw_v_cref$perc <- with(qa_raw_v_cref, round(diff / ref_pop, 4))
  qa_ref$year <- as.character(as.numeric(qa_ref$year) - 1)
  qa_raw_v_pref <- as.data.frame(inner_join(qa, qa_ref))
  qa_raw_v_pref$diff <- with(qa_raw_v_pref, round(raw_pop - ref_pop, 6))
  qa_raw_v_pref$perc <- with(qa_raw_v_pref, round(diff / ref_pop, 4))
  
  ### Write QA datasets to excel
  qa_filename <- paste0(path_raw, 
                        "/QAResults-", 
                        f_load, "-", 
                        year(Sys.Date()), 
                        str_pad(month(Sys.Date()), 2, side = "left", pad = "0"), 
                        str_pad(day(Sys.Date()), 2, side = "left", pad = "0"), 
                        ".xlsx")
  if (file.exists(qa_filename)) {
    file.remove(qa_filename)
  }
  write.xlsx(x = qa_raw_v_cref, 
             file = qa_filename,
             sheetName = "Raw Vs Cur Yr",
             col.names = T,
             row.names = F)
  write.xlsx(x = qa_raw_v_pref, 
             file = qa_filename,
             sheetName = "Raw Vs Prev Yr",
             col.names = T,
             row.names = F,
             append = T)
  message("QA Results File Complete - ", qa_filename)
  rm(f_list,  f_load,  files_to_unzip, files_in_zip, files, file_info, 
     unzipped_files, zipped_files, z_args, old_path, pop_config, data, z, y, u,
     qa_ref, qa_raw_v_cref, qa_raw_v_pref, qa_filename, qa_config, qa)
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
  for (c in 1:length(cols)) {
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
  rm(qa_config, schema_name, qa_table, ref_table, ref_table77, vars, sel_vars, cols, insert_code, c)
}
