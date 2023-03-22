#### FUNCTIONS TO DEAL WITH RAW DATA (READING FILE, LOADING INTO RAW TABLE, CLEANING DATA)
#
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2020-12

#### FUNCTION LOAD RAW DATA TO SQL RAW TABLE ####
load_raw_f <- function(
  server,
  server_dw = NULL,
  config = NULL,
  prod = F,
  interactive_auth = F,
  schema_name = NULL,
  table_name = NULL,
  file_path,
  sql_source = F,
  etl_batch_id) {
  
  ### SETTING UP VARIABLES ###
  if(is.null(server)) { 
    stop("server must be specified.") 
  }
  if(is.null(server_dw)) {
    server_dw <- server
  }
  if(server_dw == "APDEStore") { tablock <- "WITH (TABLOCK) " 
  } else { tablock <- "" }
  
  conn <- create_db_connection(server_dw, interactive = interactive_auth, prod = prod)
  
  ### Record raw data deletion ###
  cols <- get_table_cols_f(conn, schema_name, table_name)
  if(any(cols == "etl_batch_id")) {
    del_id <- DBI::dbGetQuery(conn, 
                              glue::glue_sql("sELECT TOP (1) etl_batch_id 
                                             FROM {`schema_name`}.{`table_name`}", 
                                             .con = conn))
    conn_etl <- create_db_connection(server, interactive = interactive_auth, prod = prod)
    update_etl_log_datetime_f(conn = conn_etl, etl_batch_id = as.numeric(del_id),
                              etl_schema = ref_schema, etl_table = etl_table,
                              field = "delete_raw_datetime")
  }
  
  ### Load data to raw ###
  if(sql_source == F) {
    if(server == "hhsaw") {
      if(prod == T) { dsn <- 16 
      } else { dsn <- 20 }
      copy_into_f(conn = conn, 
                server = server, 
                config = config,
                to_schema = schema_name,
                to_table = table_name,
                identity = "Storage Account Key",
                secret = key_get('inthealth_edw'),
                compression = "gzip",
                field_term = ",",
                row_term = "\n",
                first_row = 1,
                overwrite = T,
                rodbc = T,
                rodbc_dsn = paste0("int_edw_", dsn),
                dl_path = file_path)
    
    } else {
      if(file.exists(substring(file_path, 1 , nchar(file_path) - 3)) == T) {
        file.remove(substring(file_path, 1 , nchar(file_path) - 3))
      }
      gunzip(file_path, remove = F)
      create_table(conn = conn,
                 server = server,
                 config = config,
                 to_schema = schema_name,
                 to_table = table_name,
                 overwrite = T)
      load_table_from_file(conn = conn,
                         server = server,
                         config = config,
                         to_schema = schema_name,
                         to_table = table_name,
                         file_path = substring(file_path, 1, nchar(file_path) - 3),
                         first_row = 1)
      file.remove(substring(file_path, 1, nchar(file_path) - 3))
    }
  
    DBI::dbDisconnect(conn)
  
    ### GET CURRENT COLUMN ORDER ###
    conn <- create_db_connection(server_dw, interactive = interactive_auth, prod = prod)
    cols <- DBI::dbGetQuery(conn,
                          glue::glue_sql("SELECT TOP (1) col1, col2, col3, col4, col5, col6, col7
                   FROM {`schema_name`}.{`table_name`}
                   WHERE CHARINDEX('code', 
                   CONCAT(col1, col2, col3, col4, col5, col6, col7)) > 0",
                                         .con = conn))
    cols <- lapply(cols, tolower)
    for(c in 1:length(cols)) {
      if(!is.na(str_locate(cols[c], "code")[1])) {
        cols[c] <- "geo_id"
      } else if(!is.na(str_locate(cols[c], "pop")[1])) {
        cols[c] <- "pop"
      } else if(!is.na(str_locate(cols[c], "race")[1])) {
        cols[c] <- "raw_racemars"
      } else if(!is.na(str_locate(cols[c], "gender")[1])) {
        cols[c] <- "raw_gender"
      } else if(!is.na(str_locate(cols[c], "age")[1])) {
        cols[c] <- "raw_agestr"
      } else if(!is.na(str_locate(cols[c], "hispanic")[1])) {
        cols[c] <- "raw_hispanic"
      } else {
        cols[c] <- "year"
      }
    }
    DBI::dbDisconnect(conn)
  
    ### CLEAN UP RAW COLUMNS ###
    conn <- create_db_connection(server_dw, interactive = interactive_auth, prod = prod)
    DBI::dbExecute(conn,
                  glue::glue_sql("UPDATE {`schema_name`}.{`table_name`} {DBI::SQL(tablock)}
                                SET col1 = NULL, col2 = NULL, col3 = NULL, 
                                  col4 = NULL, col5 = NULL, col6 = NULL, col7 = NULL
                                WHERE CHARINDEX('code', 
                                  CONCAT(col1, col2, col3, col4, col5, col6, col7)) > 0",
                                .con = conn))
    DBI::dbDisconnect(conn)
    conn <- create_db_connection(server_dw, interactive = interactive_auth, prod = prod)
    DBI::dbExecute(conn,
                 glue::glue_sql("UPDATE {`schema_name`}.{`table_name`} {DBI::SQL(tablock)}
                                  SET col1 = RTRIM(REPLACE(col1, CHAR(13), '')), 
                                    col2 = RTRIM(REPLACE(col2, CHAR(13), '')), 
                                    col3 = RTRIM(REPLACE(col3, CHAR(13), '')), 
                                    col4 = RTRIM(REPLACE(col4, CHAR(13), '')), 
                                    col5 = RTRIM(REPLACE(col5, CHAR(13), '')), 
                                    col6 = RTRIM(REPLACE(col6, CHAR(13), '')), 
                                    col7 = RTRIM(REPLACE(col7, CHAR(13), ''))",
                                .con = conn))
    DBI::dbDisconnect(conn)
  
    ### CREATE NEW COLUMNS ###
    vars_add <- config$vars_add
    conn <- create_db_connection(server_dw, interactive = interactive_auth, prod = prod)
    for(v in 1:length(vars_add)) {
      var <- vars_add[v]
      DBI::dbExecute(conn,
                   glue::glue_sql("ALTER TABLE {`schema_name`}.{`table_name`}
                                  ADD {`names(var)`} {DBI::SQL(var)}",
                                  .con = conn))
    }
    DBI::dbDisconnect(conn)
  
    ### TRANSFER DATA TO CORRECT COLUMNS ###
    for(c in 1:length(cols)) {
      conn <- create_db_connection(server_dw, interactive = interactive_auth, prod = prod)
      if(cols[[c]] == 'pop') {
        DBI::dbExecute(conn,
                     glue::glue_sql("UPDATE {`schema_name`}.{`table_name`} {DBI::SQL(tablock)}
                                  SET {`cols[[c]]`} = CAST({`names(cols[c])`} AS FLOAT)
                                  WHERE col1 IS NOT NULL",
                                    .con = conn))
      } else {
        DBI::dbExecute(conn,
                   glue::glue_sql("UPDATE {`schema_name`}.{`table_name`} {DBI::SQL(tablock)}
                                  SET {`cols[[c]]`} = {`names(cols[c])`}
                                  WHERE col1 IS NOT NULL",
                                  .con = conn))
      }
      DBI::dbDisconnect(conn)
    }
  
    conn <- create_db_connection(server_dw, interactive = interactive_auth, prod = prod)
    DBI::dbExecute(conn,
                 glue::glue_sql("UPDATE {`schema_name`}.{`table_name`} {DBI::SQL(tablock)} 
                                SET etl_batch_id = {etl_batch_id}",
                                .con = conn))
    DBI::dbDisconnect(conn)
  } else {
    vars <- config$vars_add
    create_table(conn = conn,
                 server = server,
                 config = config,
                 to_schema = schema_name,
                 to_table = table_name,
                 vars = vars,
                 overwrite = T)
    from_table <- str_replace(file_path, "ref.", "")
    cols <- get_table_cols_f(conn = conn,
                             schema = schema_name,
                             table = from_table)
    cols <- cols$col
    geo_id <- NA
    for(x in 1:length(cols)) {
      if(instr(cols[x], "2") > 0 ||
         cols[x] == "County" ||
         cols[x] == "LEGDIST") { geo_id <- cols[x] }
    }
    info <- get_etl_log_f(conn,
                  etl_schema = ref_schema, 
                  etl_table = etl_table,
                  etl_batch_id = etl_batch_id)
    if(info$geo_scope == "kps") {
      fips_co_sql <- glue::glue_sql("SUBSTRING({`geo_id`}, 4, 2)", .con = conn)
    } else {
      fips_co_sql <- glue::glue_sql("NULL", .con = conn)
    }
    DBI::dbExecute(conn, 
                   glue::glue_sql("INSERT INTO {`schema_name`}.{`table_name`}
                                    (etl_batch_id,
                                    geo_id,
                                    pop,
                                    raw_agestr,
                                    raw_gender,
                                    raw_racemars,
                                    raw_hispanic,
                                    geo_type,
                                    geo_scope,
                                    geo_year,
                                    census_year,
                                    year,
                                    r_type,
                                    fips_co)
                                  SELECT 
                                    {etl_batch_id},
                                    {`geo_id`},
                                    Population,
                                    AgeGroup,
                                    Gender,
                                    FORMAT(RaceMars97, '00000'),
                                    Hispanic,
                                    {info$geo_type},
                                    {info$geo_scope},
                                    {info$geo_year},
                                    {info$census_year},
                                    Year,
                                    {info$r_type},
                                    {DBI::SQL(fips_co_sql)}
                                  FROM 
                                  {`schema_name`}.{`from_table`}
                                  WHERE year = {info$year}",
                                  .con = conn))
    
  }
  
  conn <- create_db_connection(server_dw, interactive = interactive_auth, prod = prod)
  qa_results <- get_row_pop_f(conn, schema_name, table_name, etl_batch_id)
  conn_etl <- create_db_connection(server, interactive = interactive_auth, prod = prod)
  update_etl_log_datetime_f(conn = conn_etl, etl_batch_id = etl_batch_id,
                            etl_schema = ref_schema, etl_table = etl_table,
                            field = "load_raw_datetime")
  qa_etl_f(conn = conn_etl, etl_batch_id = etl_batch_id,
           etl_schema = ref_schema, etl_table = etl_table,
           qa_val = qa_results$row_cnt, field = "qa_rows_raw")
  qa_etl_f(conn = conn_etl, etl_batch_id = etl_batch_id,
           etl_schema = ref_schema, etl_table = etl_table,
           qa_val = qa_results$pop_tot, field = "qa_pop_raw")
  DBI::dbDisconnect(conn)
  DBI::dbDisconnect(conn_etl)
}

#### FUNCTION TO CLEAN RAW DATA IN R AND RETURN CLEANED DATAFRAME ####
clean_raw_df_f <- function(
  conn,
  server = NULL,
  config = NULL,
  xwalk_schema = NULL,
  xwalk_table = NULL,
  hra_table = NULL,
  df,
  info,
  etl_batch_id = 0) {

  ### SETTING UP VARIABLES ###
  if((is.null(server) || is.null(config)) 
     & (is.null(xwalk_schema) || is.null(xwalk_table))) { 
    stop("config and server OR xwalk_schema and xwalk_table must be specified.") 
  }
  if((is.null(server) || is.null(config)) 
     & (is.null(xwalk_schema) || is.null(hra_table))) { 
    stop("config and server OR xwalk_schema and hra_table must be specified.") 
  }
  if(is.null(xwalk_schema)) { xwalk_schema <- config[[server]]$ref_schema }
  if(is.null(xwalk_table)) { xwalk_table <- config$crosswalk_table }
  if(is.null(hra_table)) { hra_table <- config$hra_table }
  
  ### Get population and hra crosswalk
  crosswalk <- DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT * FROM {`xwalk_schema`}.{`xwalk_table`}",
    .con = conn))
  hra <- DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT * FROM {`xwalk_schema`}.{`hra_table`}",
    .con = conn))
  
  ### CHANGE ORIGINAL COLUMN NAMES ###
  colnames(df) <- lapply(colnames(df), tolower)
  for (x in 1:length(colnames(df))) {
    if (grepl("pop", colnames(df)[x]) == T) { 
      colnames(df)[x] = "pop"
    } else if (grepl("racemars", colnames(df)[x]) == T) {
      colnames(df)[x] = "raw_racemars"
    }
    else if (grepl("age", colnames(df)[x]) == T) {
      colnames(df)[x] = "raw_agestr"
    }
    else if (grepl("code", colnames(df)[x]) == T) {
      colnames(df)[x] = "geo_id"
    }
    else if (grepl("gender", colnames(df)[x]) == T) {
      colnames(df)[x] = "raw_gender"
    }
    else if (grepl("hispanic", colnames(df)[x]) == T) {
      colnames(df)[x] = "raw_hispanic"
    }
  }
  
  ### SET FIPS_CO COLUMN AND REMOVE DATA BASED ON GEO_SCOPE ###
  df$geo_id <- as.character(df$geo_id)
  if(info$geo_type == "blk") {
    df$fips_co <- with(df, as.numeric(substring(geo_id, 3, 5)))
  } else {
    df$fips_co <- NA
  }
  if(info$geo_scope == "kps") { 
    df <- filter(df, fips_co %in% c(33, 53, 61))
  }
  
  ### SET COLUMNS BASED ON FILE INFO ###
  df$geo_type <- info$geo_type
  df$geo_scope <- info$geo_scope
  df$geo_year <- info$geo_year
  df$census_year <- info$census_year
  df$r_type <- info$r_type
  df$year <- as.character(df$year)
  
  ### FIX RACEMARS TO HAVE LEADING ZEROES ###
  df$raw_racemars <- as.character(df$raw_racemars)
  if(info$r_type == 97) {
    df$raw_racemars <- str_pad(df$raw_racemars, 5, side = "left", pad = "0")
  }
  
  ### FIX AGESTR LENGTH ###
  df$raw_agestr <- with(df, substring(raw_agestr, 1, 3))
  
  ### SET AGE ###
  df$age <- with(df, as.numeric(raw_agestr))

  ### Reorder Crosswalk so that rcode is set before r1r3 and r2r4
  #  and that r1r3 and r2r4 are set before using raw_hispanic to override
  crosswalk <- crosswalk %>% arrange(desc(new_column), desc(old_column), old_value_txt, old_value_num_min)
  
  ### Add new columns based on crosswalk ###
  for(z in 1:nrow(crosswalk)) {
    x <- crosswalk[z, ]
    # Add new column if it does not exist
    if(!(x$new_column %in% colnames(df))) {
      df[x$new_column] <- NA
    }
    # Determine new value
    if(!is.na(x$new_value_txt)) {
      new_value <- x$new_value_txt 
    } else {
      new_value <- x$new_value_num
    }
    # Check if crosswalk uses r_type
    if(!is.na(x$r_type)) {
      rtype <- " & df$r_type == x$r_type"
    } else {
      rtype <- ""
    }
    # Set new column value based on whether old value is txt or num
    if(!is.na(x$old_value_txt)) {
      eval(parse(text = glue::glue("df${x$new_column}[df${x$old_column} == x$old_value_txt{rtype}] <- new_value")))
    } else {
      eval(parse(text = glue::glue("df${x$new_column}[df${x$old_column} >= x$old_value_num_min & df${x$old_column} <= x$old_value_num_max{rtype}] <- new_value")))
    }
  }
  # Change NA to zero
  df[is.na(df)] <- 0

  ### SET HRA ###
  if(info$geo_type == 'blk') {
    hra <- hra %>%
      filter(geo_year == info$geo_year) %>%
      select(-geo_year)
    colnames(hra) <- c("geo_id", "hra_id")
    suppressMessages(df <- df %>%
      left_join(hra))
  }
  
  ### SET ETL_BATCH_ID AND ID IF PRESENT ###
  if (etl_batch_id > 0) {
    df <- cbind(etl_batch_id, df)
    df <- tibble::rowid_to_column(df, "id")
  }
  return(as.data.frame(df))
}

#### FUNCTION TO CLEAN RAW DATA IN SQL ####
clean_raw_sql_f <- function(
  server,
  server_dw = NULL,
  config = NULL,
  prod = F,
  interactive_auth = F,
  schema_name = NULL,
  table_name = NULL,
  vars_add = NULL,
  info,
  etl_batch_id) {
  
  ### SETTING UP VARIABLES ###
  if(is.null(server)) { 
    stop("server must be specified.") 
  }
  if(is.null(server_dw)) {
    server_dw <- server
  }

  if(server_dw == "APDEStore") { tablock <- "WITH (TABLOCK) " 
  } else { tablock <- "" }
  
  ### SET FILE INFO COLUMNS ###
  conn <- create_db_connection(server_dw, interactive = interactive_auth, prod = prod)
  cols <- DBI::dbGetQuery(conn,
                          glue::glue_sql("SELECT TOP (1) *
                                         FROM {`schema_name`}.{`table_name`}",
                                         .con = conn))
  DBI::dbDisconnect(conn)
  cols <- names(vars_add)
  for(col in cols) {
    if(!is.null(info[1,col])) {
      conn <- create_db_connection(server_dw, interactive = interactive_auth, prod = prod)
      DBI::dbExecute(conn,
                     glue::glue_sql("UPDATE {`schema_name`}.{`table_name`} {DBI::SQL(tablock)}
                                    SET {`col`} = {info[1,col]}",
                                    .con = conn))
      DBI::dbDisconnect(conn)
    }
  }
  
  ### SET FIPS_CO COLUMN ###
  conn <- create_db_connection(server_dw, interactive = interactive_auth, prod = prod)
  DBI::dbExecute(conn,
                 glue::glue_sql("UPDATE {`schema_name`}.{`table_name`} {DBI::SQL(tablock)}
                                SET fips_co = SUBSTRING(geo_id, 4, 2)
                                WHERE geo_scope = 'kps'",
                                .con = conn))
  DBI::dbDisconnect(conn)
}


#### FUNCTION GET INFO FROM RAW FILENAME ####
get_raw_file_info_f <- function(
  config,
  file_name) {
  
  file_name <- tolower(file_name)
  if (str_detect(file_name, ".gz")) {
    file_name <- substring(file_name, 1, nchar(file_name) - 3)
  }
  geo_type <- substring(file_name, 1, str_locate(file_name, "racemars")[1,1] - 5)
  geo_type <- str_replace(geo_type, "code", "")
  geo_year <- substring(file_name, str_locate(file_name, "racemars")[1,1] - 4, str_locate(file_name, "racemars")[1,1] - 1)
  year <- substring(file_name, nchar(file_name) - 11, nchar(file_name) - 8)
  r_type <- as.integer(substring(file_name, nchar(file_name) - 13, nchar(file_name) - 12))
  geo_types_df <- as.data.frame(config$geo_type)
  geo_types <- colnames(geo_types_df)
  for (gt in geo_types) {
    if (gt == geo_type) {
      geo_type <- geo_types_df[1, gt]
    }
  }
  geo_scopes_df <- as.data.frame(config[["geo_scope"]])
  geo_scopes <- colnames(geo_scopes_df)
  geo_scope <- NA
  for (gs in geo_scopes) {
    if (gs == geo_type) {
      geo_scope <- geo_scopes_df[1, gs]
    }
  }
  return(data.frame(geo_type, geo_scope, geo_year, year, r_type))
}

#### FUNCTION TO DETERMINE HOW MANY ROWS HAVE BEEN LOADED TO RAW WHEN LOAD FAILS ####
failed_raw_load_f <- function(
  conn,
  schema_name,
  table_name,
  etl_batch_id) {
  rows_loaded  <- 0
  if (DBI::dbExistsTable(conn, DBI::Id(schema = schema_name, table = table_name))) {
    sql_get <- glue::glue_sql(
      "SELECT etl_batch_id, COUNT(*) 
      FROM {`schema_name`}.{`table_name`} 
      WHERE etl_batch_id = {etl_batch_id}
      GROUP BY etl_batch_id",
      .con = conn)
    results <- DBI::dbGetQuery(conn, sql_get)
    if (nrow(results) > 0) { rows_loaded <- results[1,2] }
  } 
  return(rows_loaded)
}
