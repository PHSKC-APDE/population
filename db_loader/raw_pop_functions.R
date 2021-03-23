#### FUNCTIONS TO DEAL WITH RAW DATA (READING FILE, LOADING INTO RAW TABLE, CLEANING DATA)
#
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2020-12

#### FUNCTION LOAD RAW DATA TO SQL####
load_raw_f <- function(
  conn,
  schema_name,
  table_name,
  data,
  etl_batch_id) {
  
  ### LOAD DATA TO REF
  load_data_f(conn, data, schema_name, table_name)
  
  ### GETS NUMBER OF ROWS LOADED TO SQL AND RETURNS THE NUMBER
  pop_load <- DBI::dbGetQuery(conn, 
                              glue::glue_sql(
                                "SELECT pop
                         FROM {`schema_name`}.{`table_name`}
                         WHERE etl_batch_id = {etl_batch_id}",
                                .con = conn))
  data_loaded$rows <- nrow(pop_load)
  data_loaded$pop <- as.numeric(pop_load %>% summarize_at(vars(pop), list(tot_pop = sum)))
  
  return(data_loaded)
}

#### FUNCTION TO CLEAN RAW DATA IN R AND RETURN CLEANED DATAFRAME ####
clean_raw_r_f <- function(
  conn,
  config,
  df,
  info,
  etl_batch_id = 0) {

  ### Get population and hra crosswalk
  crosswalk <- DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT * FROM {`config$ref_schema`}.{`config$crosswalk_table`}",
    .con = conn))
#  hra <- DBI::dbGetQuery(conn, glue::glue_sql(
#    "SELECT * FROM {`config$ref_schema`}.{`config$hra_table`}",
#    .con = conn))
  
  ### CHANGE ORIGINAL COLUMN NAMES ###
  colnames(df) <- lapply(colnames(df), tolower)
  for (x in 1:length(colnames(df))) {
    if (grepl("pop", colnames(df)[x]) == T) { 
      colnames(df)[x] = "pop"
    } else if (grepl("racemars", colnames(df)[x]) == T) {
      colnames(df)[x] = "racemars"
    }
    else if (grepl("age", colnames(df)[x]) == T) {
      colnames(df)[x] = "agestr"
    }
    else if (grepl("code", colnames(df)[x]) == T) {
      colnames(df)[x] = "geo_id"
    }
  }
  
  ### SET FIPS_CO COLUMN AND REMOVE DATA BASED ON GEO_SCOPE ###
  df$geo_id <- as.character(df$geo_id)
  if(info$geo_scope == "kps") {
    df$fips_co <- with(df, as.numeric(substring(geo_id, 3, 5)))
    df <- filter(df, fips_co %in% c(33, 53, 61))
  } else {
    df$fips_co <- NA
  }
  
  ### SET COLUMNS BASED ON FILE INFO ###
  df$geo_type <- info$geo_type
  df$geo_scope <- info$geo_scope
  df$geo_year <- info$geo_year
  df$r_type <- info$r_type
  df$year <- as.character(df$year)
  
  ### FIX RACEMARS TO HAVE LEADING ZEROES ###
  df$racemars <- as.character(df$racemars)
  if(info$r_type == 97) {
    df$racemars <- str_pad(df$racemars, 5, side = "left", pad = "0")
  }
  
  ### FIX AGESTR LENGTH ###
  df$agestr <- with(df, substring(agestr, 1, 3))
  
  ### SET AGE ###
  df$age <- with(df, as.numeric(agestr))
  
  ### SET AGE5 ###
  xwalk <- filter(crosswalk, new_column == "age5")
  xwalk <- select(xwalk, old_value_num_min, old_value_num_max, new_value_num)
  colnames(xwalk) <- c("min", "max", "age5")
  df <- df %>% 
    mutate(dummy = TRUE) %>%
    left_join(xwalk %>% mutate(dummy = TRUE)) %>%
    filter(age >= min, age <= max) %>%
    select(-dummy, -min, -max)
  
  ### SET AGE11 ###
  xwalk <- filter(crosswalk, new_column == "age11")
  xwalk <- select(xwalk, old_value_num_min, old_value_num_max, new_value_num)
  colnames(xwalk) <- c("min", "max", "age11")
  df <- df %>% 
    mutate(dummy = TRUE) %>%
    left_join(xwalk %>% mutate(dummy = TRUE)) %>%
    filter(age >= min, age <= max) %>%
    select(-dummy, -min, -max)
  
  ### SET AGE20 ###
  xwalk <- filter(crosswalk, new_column == "age20")
  xwalk <- select(xwalk, old_value_num_min, old_value_num_max, new_value_num)
  colnames(xwalk) <- c("min", "max", "age20")
  df <- df %>% 
    mutate(dummy = TRUE) %>%
    left_join(xwalk %>% mutate(dummy = TRUE)) %>%
    filter(age >= min, age <= max) %>%
    select(-dummy, -min, -max)
  
  ### SET S (GENDER) ###
  xwalk <- filter(crosswalk, new_column == "s")
  xwalk <- select(xwalk, old_value_txt, new_value_num)
  colnames(xwalk) <- c("gender", "s")
  df <- inner_join(df, xwalk, by = "gender")
  
  ### SET H (HISPANIC) ###
  xwalk <- filter(crosswalk, new_column == "h",)
  xwalk <- select(xwalk, old_value_txt, new_value_num)
  colnames(xwalk) <- c("hispanic", "h")
  xwalk$hispanic <- as.numeric(xwalk$hispanic)
  df <- inner_join(df, xwalk, by = "hispanic")
  
  ### SET RCODE ###
  xwalk <- filter(crosswalk, new_column == "rcode", r_type == info$r_type)
  xwalk <- select(xwalk, old_value_txt, new_value_num)
  colnames(xwalk) <- c("racemars", "rcode")
  df <- inner_join(df, xwalk, by = "racemars")
  
  ### SET R1_3 ###
  xwalk <- filter(crosswalk, new_column == "r1_3", r_type == info$r_type, old_column == "rcode")
  xwalk <- select(xwalk, old_value_num_min, old_value_num_max, new_value_num)
  colnames(xwalk) <- c("min", "max", "r1_3")
  df <- df %>% 
    mutate(dummy = TRUE) %>%
    left_join(xwalk %>% mutate(dummy = TRUE)) %>%
    filter(rcode >= min, rcode <= max) %>%
    select(-dummy, -min, -max)
  
  ### SET R2_4 AND UPDATE BASED ON H ###
  xwalk <- filter(crosswalk, new_column == "r2_4", r_type == info$r_type, old_column == "rcode")
  xwalk <- select(xwalk, old_value_num_min, old_value_num_max, new_value_num)
  colnames(xwalk) <- c("min", "max", "r")
  df <- df %>% 
    mutate(dummy = TRUE) %>%
    left_join(xwalk %>% mutate(dummy = TRUE)) %>%
    filter(rcode >= min, rcode <= max) %>%
    select(-dummy, -min, -max)
  xwalk <- filter(crosswalk, new_column == "r2_4", r_type == info$r_type, old_column == "h")
  xwalk <- select(xwalk, old_value_num_min, new_value_num)
  colnames(xwalk) <- c("h", "r2_4")
  df <- left_join(df, xwalk, by = "h")
  df$r2_4 <- ifelse(is.na(df$r2_4), df$r, df$r2_4)
  df <- select(df, -r)
  
  ### SET HRA ###
#  if(info$geo_type == 'blk') {
#    hra <- hra %>%
#      filter(geo_year == info$geo_year) %>%
#      select(-geo_year)
#    colnames(hra) <- c("geo_id", "hra_id")
#    df <- df %>%
#      left_join(hra)
#  }
  
  ### ORDER COLUMNS ###
  df <- df[, c("geo_type", "geo_scope", "geo_year", "year", 
               "r_type", "geo_id", "age", "age5", "age11", "age20",
               "s", "h", "rcode", "r1_3", "r2_4", "pop", "fips_co", 
               "agestr", "gender","racemars", "hispanic")]
#               "agestr", "gender","racemars", "hispanic", "hra_id")]
  
  ### SET ETL_BATCH_ID AND ID IF PRESENT ###
  if (etl_batch_id > 0) {
    df <- cbind(etl_batch_id, df)
    df <- tibble::rowid_to_column(df, "id")
  }
  return(as.data.frame(df))
}

#### FUNCTION GET INFO FROM RAW FILENAME ####
get_raw_file_info_f <- function(
  config,
  file_name) {
  
  file_name <- tolower(file_name)
  geo_type <- substr(file_name, 1, instr(file_name, "racemars") - 5)
  geo_year <- substr(file_name, instr(file_name, "racemars") - 4, instr(file_name, "racemars") - 1)
  year <- substr(file_name, nchar(file_name) - 11, nchar(file_name) - 8)
  r_type <- as.integer(substr(file_name, nchar(file_name) - 13, nchar(file_name) - 12))
  geo_types_df <- as.data.frame(config$geo_type)
  geo_types <- colnames(geo_types_df)
  for (i in 1:length(geo_types)) {
    if (geo_types[i] == geo_type) {
      geo_type <- geo_types_df[1,i]
    }
  }
  geo_scopes_df <- as.data.frame(config[["geo_scope"]])
  geo_scopes <- colnames(geo_scopes_df)
  geo_scope <- NA
  for (i in 1:length(geo_scopes)) {
    if (geo_scopes[i] == geo_type) {
      geo_scope <- geo_scopes_df[1,i]
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
