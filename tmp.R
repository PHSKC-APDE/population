
config <- yaml::yaml.load_file("C:/Users/jwhitehurst/OneDrive - King County/GitHub/population/config/common.pop.yaml")
rawconfig <- yaml::yaml.load_file("C:/Users/jwhitehurst/OneDrive - King County/GitHub/population/config/raw.pop.yaml")
file_info <- get_raw_file_info_f(config = config, file_name = "ZipCode2019RaceMars972019_csv.csv.gz")

conn_db <- create_db_connection("hhsaw", interactive = interactive_auth, prod = prod)
conn_dw <- create_db_connection("inthealth", interactive = interactive_auth, prod = prod)
dl_path <- "https://inthealthdtalakegen2.blob.core.windows.net/inthealth/apde/pop/20201223prelim/csv_2019/Block2010RaceMars972019_csv.csv.gz"
copy_into_f(conn = conn_dw, 
            server = "hhsaw", 
            config = rawconfig,
            identity = "Storage Account Key",
            secret = key_get('inthealth_edw'),
            compression = "gzip",
            field_terminator = ",",
            first_row = 1,
            overwrite = T,
            rodbc = T,
            rodbc_dsn = "int_edw_20",
            dl_path = dl_path)






conn_db <- create_db_connection("APDEStore")
create_table(conn = conn_db,
             server = "APDEStore",
             config = rawconfig)
file_path <- "//phdata01/DROF_DATA/DOH DATA/POP/data/gz/20200616revised/csv_2018/ZipCode2018RaceMars972018_csv.csv"
load_table_from_file(conn = conn_db,
                     server = "APDEStore",
                     config = rawconfig,
                     file_path = file_path,
                     first_row = 1)



create_table(conn = conn_db,
             to_schema = config[[server]]$ref_schema,
             to_table = paste0("archive_", config[[server]]$table_name),
             vars = config$vars)
create_table(conn = conn_db,
             to_schema = config[[server]]$ref_schema,
             to_table = paste0("archive_", config[[server]]$table_name,"_77"),
             vars = config$vars)
create_table(conn = conn_db,
             to_schema = config[[server]]$ref_schema,
             to_table = config[[server]]$table_name,
             vars = config$vars)
create_table(conn = conn_db,
             to_schema = config[[server]]$ref_schema,
             to_table = paste0(config[[server]]$table_name, "_77"),
             vars = config$vars)

conn <- create_db_connection("hhsaw", interactive = T, prod = T)
conn_d <- create_db_connection("hhsaw", interactive = T, prod = F)
conn_a <- create_db_connection("APDEStore", interactive = T, prod = F)

df <- dbGetQuery(conn_p, "SELECT * FROM ref.pop_crosswalk 
                 ORDER BY new_column, old_column, old_value_txt, old_value_num_min")
df <- df[df$r_type == 77 | is.na(df$r_type), ]

for(y in 1:nrow(df)) {
  z <- df[y, ]
  sql <- "UPDATE ref.pop_77 WITH (TABLOCK) SET "
  sql <- paste0(sql, z$new_column, " = ")
  if(!is.na(z$new_value_num)) { 
    sql <- paste0(sql, z$new_value_num)  
  } else { 
    sql <- paste0(sql, "'", z$new_value_txt, "'") 
  }
  sql <- paste0(sql, " WHERE ")
  if(!is.na(z$old_value_txt)) { 
    sql <- paste0(sql, z$old_column, " = '", z$old_value_txt, "' ") 
  } else { 
    sql <- paste0(sql, "(", z$old_column, " >= ", z$old_value_num_min, "AND ", z$old_column, " <= ", z$old_value_num_max, ")") 
    }
  if (!is.na(z$r_type)) { 
    sql <- paste0(sql, " AND r_type = ", z$r_type) 
  }
  sql <- paste0(sql, " AND ", z$new_column, " IS NULL")
  message(paste0(y, " - ", z$new_column))
  conn_a <- create_db_connection("APDEStore", interactive = T, prod = F)
  dbExecute(conn_a, sql)
}


