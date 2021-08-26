#### FUNCTIONS FOR DATA
#
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2020-12


#### FUNCTION TO GET ROW COUNT AND POP TOTAL ####
get_row_pop_f <- function(
  conn,
  schema_name,
  table_name,
  etl_batch_id) {
  df <- DBI::dbGetQuery(conn, glue::glue_sql(
    "SELECT COUNT(pop) AS 'row_cnt',
      ROUND(SUM(pop), 0) AS 'pop_tot'
    FROM {`schema_name`}.{`table_name`}
    WHERE etl_batch_id = {etl_batch_id}
      AND pop IS NOT NULL", 
    .con = conn))
  return(df)
}

#### FUNCTION TO MOVE DATA FROM ONE TABLE TO ANOTHER ####
data_move_f <- function(
  conn,
  from_schema,
  to_schema,
  from_table,
  to_table,
  overwrite = F,
  etl_batch_id = NULL,
  del_from = F,
  del_to = F,
  where = "") {
  
  if(server == "APDEStore") { tablock <- "WITH (TABLOCK) " 
  } else { tablock <- "" }
  
  ### Removes data from the to_table with matching etl_batch_id (should be none) ###
  if(del_to == T) {
    DBI::dbExecute(conn, glue_sql("DELETE FROM {`to_schema`}.{`to_table`} {DBI::SQL(tablock)}
                                  WHERE etl_batch_id = {etl_batch_id}", .con = conn))
  }
  
  ### Remove all data from to_table if overwrite is TRUE ###
  if(overwrite == T) {
    DBI::dbExecute(conn, glue_sql("TRUNCATE TABLE {`to_schema`}.{`to_table`}", 
                                  .con = conn))
  }
  ### Gets list of columns from tables and find matching columns
  cols_from <- get_table_cols_f(conn = conn, schema = from_schema, table = from_table)
  cols_to <- get_table_cols_f(conn = conn, schema = to_schema, table = to_table)
  cols_from$col <- tolower(cols_from$col)
  cols_to$col <- tolower(cols_to$col)
  cols <- suppressMessages(dplyr::semi_join(cols_from, cols_to))
  
  ### Where Clause ###
  if(where != "") {
    where <- paste0 ("AND ", where)
  }
  
  ### Insert data from the from table into the to table ###
  DBI::dbExecute(conn, glue::glue_sql(
    "INSERT INTO {`to_schema`}.{`to_table`} {DBI::SQL(tablock)}
    ({DBI::SQL(glue::glue_collapse(glue::glue_sql('{`cols$col`}', .con = conn), sep = ', '))})
    SELECT {DBI::SQL(glue::glue_collapse(glue::glue_sql('{`cols$col`}', .con = conn), sep = ', '))}
    FROM {`from_schema`}.{`from_table`}
    WHERE etl_batch_id = {etl_batch_id} AND etl_batch_id IS NOT NULL AND pop IS NOT NULL
      {DBI::SQL(where)}", 
    .con = conn))
  
  ### Removes data from the from_table###
  if(del_from == T) {
    DBI::dbExecute(conn, glue_sql("DELETE FROM {`from_schema`}.{`from_table`} {DBI::SQL(tablock)}
                                  WHERE etl_batch_id = {etl_batch_id}", .con = conn))
  }
}

### FUNCTION TO MOVE DATA FROM ONE TABLE TO ANOTHER THAT PULLS DATA INTO R AND THEN UPLOADS TO NEW TABLE
data_move_r_f <- function(
  conn,
  to_schema,
  from_schema,
  table_name,
  etl_batch_id) {
  
  ### Removes data from the to table
  message(paste0("DATA MOVE - Checking for and removing data from ", 
                 to_schema, ".", table_name))
  DBI::dbExecute(conn, 
                 glue_sql(
                   "DELETE FROM {`to_schema`}.{`table_name`} WITH (TABLOCK) 
                   WHERE etl_batch_id = {etl_batch_id}", .con = conn))
 
  ### Get data from the from table
  message(paste0("DATA MOVE - Pulling data from ", from_schema, ".", table_name))
  data <- DBI::dbGetQuery(conn, glue_sql("SELECT * 
                                         FROM {`from_schema`}.{`table_name`} 
                                         WHERE etl_batch_id = {etl_batch_id}", 
                                         .con = conn))
  
  ### Insert data from dataframe to the to table
  message(paste0("DATA MOVE - Loading data to ", to_schema, ".", table_name))
  load_data_f(conn, data, to_schema, table_name)
  
  ### Removes data from the from table
  message(paste0("DATA MOVE - Removing data from ", from_schema, ".", table_name))
  DBI::dbExecute(conn, 
                 glue_sql(
                   "DELETE FROM {`from_schema`}.{`table_name`} WITH (TABLOCK) 
                   WHERE etl_batch_id = {etl_batch_id}", .con = conn))
  
  rm(data, d, d_start, d_end, d_stop, inc)
}

load_data_f <- function(conn,
                        data,
                        schema_name,
                        table_name){
  ### Set data loading increment size and data stop variables
  inc <- 100000
  d_stop <- as.integer(nrow(data) / inc)
  if (d_stop * inc < nrow(data)) { d_stop <- d_stop + 1 }
  message(paste0("...Loading Progress - 0%"))
  
  ### Begin data loading loop
  for( d in 1:d_stop) {
    ### Re-establishing the database connection
    conn <- create_conn_f(server = server,
                          prod = prod_serv)
    d_start <- ((d - 1) * inc) + 1
    d_end <- d * inc
    if (d_end > nrow(data)) { d_end <- nrow(data) }
    dbAppendTable(conn, 
                  name = DBI::Id(schema = schema_name, table = table_name), 
                  value = data[d_start:d_end,])  
    message(paste0("...Loading Progress - ", round((d / d_stop) * 100, 2), "%"))
    conn <- create_conn_f(server = server,
                          prod = prod_serv)
  }
  rm(d, d_start, d_end, d_stop, inc)
  conn <- create_conn_f(server = server,
                        prod = prod_serv)
}