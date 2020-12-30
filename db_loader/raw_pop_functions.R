#### FUNCTIONS TO DEAL WITH RAW DATA (READING FILE, LOADING INTO RAW TABLE, CLEANING DATA)
#
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2020-12

#### FUNCTION LOAD RAW DATA TO SQL####
load_raw_f <- function(
  conn,
  config,
  path_tmp,
  data,
  etl_batch_id) {

  file_tmp <- paste0(path_tmp, "/tmp.txt")
  if (grepl(":", file_tmp) == T) { file_load <- gsub("/","\\\\",file_tmp) 
  } else { file_load <- file_tmp }
  
  write.table(data, file = file_tmp, sep = "\t", eol = "\n", quote = F, row.names = F, col.names = T)
  create_table_f(conn = conn, config = config)
  bcp_args <- c(glue(' PH_APDEStore.{config$schema_name}.{config$table_name} IN ', 
                     ' "{file_load}" ',
                     ' -t {config$field_term} -r {config$row_term} -C 65001 -F 2 ',
                     ' -S KCITSQLUTPDBH51 -T -b 100000 -c '))
  
  system2(command = "bcp", args = c(bcp_args))
  
  sql_get <- glue::glue_sql(
    "SELECT etl_batch_id, COUNT(*) 
      FROM {`config$schema_name`}.{`{config$table_name}`} 
      WHERE etl_batch_id = {etl_batch_id}
      GROUP BY etl_batch_id",
    .con = conn)
  
  rows_loaded <- DBI::dbGetQuery(conn, sql_get)
  
  for(i in 1:nrow(rows_loaded)) {
    update_etl_log_datetime_f(
      conn = conn, 
      etl_batch_id = rows_loaded[i, 1],
      field = "load_raw_datetime")
  }
  file.remove(file_tmp)
  return(rows_loaded)
}

clean_raw_f <- function(
  conn,
  config) {
  
  etl_schema <- "metadata"
  etl_table <- "pop_etl_log"
  
  alter_table_f(conn = conn, config = config)
  
  ### UPDATE FIELDS BASED ON ETL LOG INFO ###
  DBI::dbExecute(conn,glue::glue_sql(
    "UPDATE R
    SET R.geo_type = E.geo_type, 
    R.geo_scope = E.geo_scope, 
    R.geo_year = E.geo_year, 
    R.r_type = E.r_type
    FROM {`config$schema_name`}.{`config$table_name`} R
    INNER JOIN {`etl_schema`}.{`etl_table`} E ON R.etl_batch_id = E.id
    WHERE R.geo_type IS NULL",
    .con = conn))
  
  ### FIX RACEMARS TO HAVE LEADING ZEROES ###
  DBI::dbExecute(conn,glue::glue_sql(
    "UPDATE {`config$schema_name`}.{`config$table_name`}
    SET racemars = RIGHT('0000'+ CAST(racemars AS VARCHAR(5)), 5)
    WHERE LEN(racemars) < 5 AND r_type = 97",
    .con = conn))
  
  ### FIX AGESTR ###
  DBI::dbExecute(conn,glue::glue_sql(
    "UPDATE {`config$schema_name`}.{`config$table_name`}
    SET agestr = LEFT(agestr, 3)
    WHERE LEN(agestr) > 3",
    .con = conn))
  
  ### SET AGE ###
  DBI::dbExecute(conn,glue::glue_sql(
    "UPDATE {`config$schema_name`}.{`config$table_name`}
    SET age = CAST(agestr AS SMALLINT)
    WHERE age IS NULL",
    .con = conn))
  
  ### SET AGE11 USE REF.POP_CROSSWALK ###
  DBI::dbExecute(conn,glue::glue_sql(
    "UPDATE R
    SET R.age11 = X.new_value_num
    FROM {`config$schema_name`}.{`config$table_name`} R
    INNER JOIN ref.pop_crosswalk X ON X.old_value_num_min <= R.age 
      AND X.old_value_num_max >= R.age
    WHERE X.new_column = 'age11' AND X.old_column = 'age' AND R.age11 IS NULL",
    .con = conn))
  
  ### SET AGE20 USE REF.POP_CROSSWALK###
  DBI::dbExecute(conn,glue::glue_sql(
    "UPDATE R
    SET R.age20 = X.new_value_num
    FROM {`config$schema_name`}.{`config$table_name`} R
    INNER JOIN ref.pop_crosswalk X ON X.old_value_num_min <= R.age 
      AND X.old_value_num_max >= R.age
    WHERE X.new_column = 'age20' AND X.old_column = 'age' AND R.age20 IS NULL",
    .con = conn))
  
  ### SET S USE REF.POP_CROSSWALK ###
  DBI::dbExecute(conn,glue::glue_sql(
    "UPDATE R
    SET R.s = X.new_value_num
    FROM {`config$schema_name`}.{`config$table_name`} R
    INNER JOIN ref.pop_crosswalk X ON X.old_value_txt = R.gender 
    WHERE X.new_column = 's' AND X.old_column = 'gender' AND R.s IS NULL",
    .con = conn))
  ### SET H USE REF.POP_CROSSWALK ###
  DBI::dbExecute(conn,glue::glue_sql(
    "UPDATE R
    SET R.h = X.new_value_num
    FROM {`config$schema_name`}.{`config$table_name`} R
    INNER JOIN ref.pop_crosswalk X ON X.old_value_txt = R.hispanic 
    WHERE X.new_column = 'h' AND X.old_column = 'hispanic' AND R.h IS NULL",
    .con = conn))
  ### SET RCODE USE REF.POP_CROSSWALK ###
  DBI::dbExecute(conn,glue::glue_sql(
    "UPDATE R
    SET R.rcode = X.new_value_num
    FROM {`config$schema_name`}.{`config$table_name`} R
    INNER JOIN ref.pop_crosswalk X ON X.old_value_txt = R.racemars 
      AND R.r_type = X.r_type
    WHERE X.new_column = 'rcode' AND X.old_column = 'racemars' AND R.rcode IS NULL",
    .con = conn))
  
  ### SET R1_3 USE REF.POP_CROSSWALK ###
  DBI::dbExecute(conn,glue::glue_sql(
    "UPDATE R
    SET R.r1_3 = X.new_value_num
    FROM {`config$schema_name`}.{`config$table_name`} R
    INNER JOIN ref.pop_crosswalk X ON X.old_value_num_min <= R.rcode 
      AND X.old_value_num_max >= R.rcode AND R.r_type = X.r_type
    WHERE X.new_column = 'r1_3' AND X.old_column = 'rcode' AND R.r1_3 IS NULL",
    .con = conn))
  
  ### SET R2_4 USE REF.POP_CROSSWALK ###
  DBI::dbExecute(conn,glue::glue_sql(
    "UPDATE R
    SET R.r2_4 = X.new_value_num
    FROM {`config$schema_name`}.{`{config$table_name}`} R
    INNER JOIN ref.pop_crosswalk X ON X.old_value_num_min <= R.rcode 
      AND X.old_value_num_max >= R.rcode AND R.r_type = X.r_type
    WHERE X.new_column = 'r2_4' AND X.old_column = 'rcode' AND R.r2_4 IS NULL",
    .con = conn))
  DBI::dbExecute(conn,glue::glue_sql(
    "UPDATE {`config$schema_name`}.{`config$table_name`}
    SET r2_4 = 6 WHERE h = 1",
    .con = conn))
  
  ### SET FIPS_CO COLUMN ###
  DBI::dbExecute(conn,glue::glue_sql(
    "UPDATE {`config$schema_name`}.{`{config$table_name}`}
    SET fips_co = CAST(SUBSTRING(geo_id, 3, 3) AS SMALLINT)
    WHERE fips_co IS NULL AND geo_type IN('blk', 'cou')",
    .con = conn))
}

#### FUNCTION GET INFO FROM RAW FILENAME ####
get_raw_file_info_f <- function(
  config,
  file_name) {
  geo_type <- substr(file_name, 1, instr(file_name, "RaceMars") - 5)
  geo_year <- substr(file_name, instr(file_name, "RaceMars") - 4, instr(file_name, "RaceMars") - 1)
  year <- substr(file_name, nchar(file_name) - 11, nchar(file_name) - 8)
  r_type <- as.integer(substr(file_name, nchar(file_name) - 13, nchar(file_name) - 12))
  geo_types_df <- as.data.frame(config$geo_type)
  geo_types <- colnames(geo_types_df)
  for (i in 1:length(geo_types)) {
    if (geo_types[i] == geo_type) {
      geo_type <- geo_types_df[1,i]
    }
  }
  geo_scopes_df <- as.data.frame(pop_config[["geo_scopes"]])
  geo_scopes <- colnames(geo_types_df)
  geo_scope <- NA
  for (i in 1:length(geo_scopes)) {
    if (geo_scopes[i] == geo_type) {
      geo_scope <- geo_scopes_df[1,i]
    }
  }
  return(data.frame(geo_type, geo_scope, geo_year, year, r_type))
}


