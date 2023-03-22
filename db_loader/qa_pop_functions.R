


### FUNCTION TO SELECT BATCH AND START THE QA PROCESS
qa_raw_files_f <- function(server = "hhsaw",
                           prod = T,
                           interactive = T,
                           filelist,
                           df_only = F){
  
  conn <- create_db_connection(server = server, prod = prod, interactive = interactive)
  qa_config <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/population/master/config/qa.pop.yaml")
  
  files <- data.frame(matrix(ncol = 6, nrow = 0))
  colnames(files) <- c("file_name", "geo_type", "geo_scope", "geo_year", "year", "r_type")
  
  if(df_only == F) {
    qa_ref <- DBI::dbGetQuery(conn, glue::glue_sql(
      "SELECT * FROM {`qa_config$schema_name`}.{`qa_config$table_name`}",
      .con = conn))
    qa_ref$ref_pop <- qa_ref$pop
    qa_ref <- select(qa_ref, -pop)
  }
  data <- data.frame()
  qa <- data.frame(matrix(ncol = 8, nrow = 0))
  
  for(file in filelist) {
    message(paste0("Processing: ", file))
    file_name <- substring(file, 
                           str_locate_all(file, "/")[[1]][[nrow(str_locate_all(file, "/")[[1]])]] + 1, 
                           nchar(file))
    file_info <- get_raw_file_info_f(config = config,
                                     file_name = file_name)
    file_info$file_name <- file_name
    file_info$file_path <- file
    df <- read.csv(file)
    file_info$rows_file <- nrow(df)
    file_info$pop_file <- sum(df$Population)
    df <- clean_raw_df_f(conn = conn,
                         server = server,
                         config = config,
                         df = df,
                         info = file_info)
    for(c in qa_config$cols) {
      eval(parse(text = glue::glue("qa <- rbind(qa, df %>%
      group_by(geo_type, geo_scope, geo_year, r_type, year, col = '{c}', val = {c}) %>%
      summarize_at(vars(pop), list(raw_pop = sum)))")))
    }
    if(nrow(data) == 0) { data <- file_info }
    else { data <- rbind(data, file_info) }
  }
  
  if(df_only == F) {
    ### Create qa comparison columns and data sets
    qa_raw_v_cref <- as.data.frame(inner_join(qa, qa_ref))
    qa_raw_v_cref$diff <- with(qa_raw_v_cref, round(raw_pop - ref_pop, 6))
    qa_raw_v_cref$perc <- with(qa_raw_v_cref, round(diff / ref_pop, 4))
    qa_ref$year <- with(qa_ref, as.character(as.numeric(year) + 1))
    qa_ref$geo_year <- with(qa_ref, ifelse(geo_type == "zip", as.character(as.numeric(geo_year) + 1), geo_year))
    qa_raw_v_pref <- as.data.frame(inner_join(qa, qa_ref))
    qa_raw_v_pref$diff <- with(qa_raw_v_pref, round(raw_pop - ref_pop, 6))
    qa_raw_v_pref$perc <- with(qa_raw_v_pref, round(diff / ref_pop, 4))
    
    ### Write QA datasets to excel
    qa_filename <- paste0(temp_base, 
                          "/QAResults-", 
                          batch_name, "-", 
                          year(Sys.Date()), 
                          str_pad(month(Sys.Date()), 2, side = "left", pad = "0"), 
                          str_pad(day(Sys.Date()), 2, side = "left", pad = "0"), 
                          ".xlsx")
    if (file.exists(qa_filename)) {
      file.remove(qa_filename)
    }
    if(nrow(qa_raw_v_cref) > 0) {
      write.xlsx(x = qa_raw_v_cref, 
              file = qa_filename,
              sheetName = "Raw Vs Cur Yr",
              col.names = T,
              row.names = F)
    }
    if(nrow(qa_raw_v_pref) > 0) {
      write.xlsx(x = qa_raw_v_pref, 
              file = qa_filename,
              sheetName = "Raw Vs Prev Yr",
              col.names = T,
              row.names = F,
              append = T)
    }
    if(nrow(qa_raw_v_pref) + nrow(qa_raw_v_cref) > 0) {
      message("QA Results File Complete - ", qa_filename)
    }
  }
  return(as.data.frame(data))
}

create_qa_pop_f <- function(){
  qa_config <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/population/master/config/qa.pop.yaml")
  ref_config <- yaml::read_yaml("https://raw.githubusercontent.com/PHSKC-APDE/population/master/config/common.pop.yaml")
  schema_name <- qa_config$schema_name
  qa_table <-  qa_config$table_name
  vars <- qa_config$vars
  sel_vars <- vars[1:5]
  cols <- qa_config$cols
  servers <- dlg_list(c("APDEStore", "hhsaw"), 
                      title = "Select Server(s) to Load ETL",
                      multiple = T,
                      preselect = c("APDEStore", "hhsaw"))$res
  
  for(server in servers) {
    ref_schema <- ref_config[[server]]$ref_schema
    ref_table <- ref_config[[server]]$table_name
    ref_table77 <- paste0(ref_table, "_77")
    conn <- create_db_connection(server = server, interactive = interactive_auth, prod = prod)
    create_table(conn = conn, 
               to_schema = schema_name, 
               to_table = qa_table, 
               vars = vars)
    for (c in 1:length(cols)) {
      insert_code <- glue::glue_sql(
        "INSERT INTO {`schema_name`}.{`qa_table`} 
        ({DBI::SQL(glue::glue_collapse(glue::glue_sql('{`names(vars)`}', 
        .con = conn), sep = ', '))}) 
        SELECT 
        {DBI::SQL(glue::glue_collapse(glue::glue_sql('{`names(sel_vars)`}', 
        .con = conn), sep = ', '))}, 
        {cols[c]}, {`cols[[c]]`}, SUM(\"pop\")
        FROM {`ref_schema`}.{`ref_table`}
        WHERE {`cols[[c]]`} IS NOT NULL
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
        FROM {`ref_schema`}.{`ref_table77`}
        WHERE {`cols[[c]]`} IS NOT NULL
        GROUP BY
        {DBI::SQL(glue::glue_collapse(glue::glue_sql('{`names(sel_vars)`}', 
        .con = conn), sep = ', '))}, {`cols[[c]]`}
        ", .con = conn)
      DBI::dbExecute(conn, insert_code)
    }
  }
  rm(qa_config, schema_name, qa_table, ref_table, ref_table77, vars, sel_vars, cols, insert_code, c)
}

conn <- create_db_connection(server = "hhsaw", interactive = T, prod = T)

sel <- glue::glue_collapse(glue::glue_sql("{`names(sel_vars)`}", .con = conn), sep = ', ')
col <- glue::glue_collapse(glue::glue_sql("CAST({`cols`} AS SMALLINT) AS {cols}", .con = conn), sep = ', ')
val <- glue::glue_collapse(glue::glue_sql("{`cols`}", .con = conn), sep = ', ')
select_vars <- glue::glue_sql(
  "SELECT {DBI::SQL(sel)}, \"col\", \"val\", SUM(\"pop\") AS 'pop'
   FROM (
     SELECT {DBI::SQL(sel)}, 
       {DBI::SQL(col)}, 
       \"pop\"
     FROM [][]) AS UNPVT
   UNPIVOT(
     val FOR col IN ({DBI::SQL(val)})
   ) AS VC
   WHERE \"val\" IS NOT NULL
   GROUP BY {DBI::SQL(sel)}, \"col\", \"val\"", .con = conn)
