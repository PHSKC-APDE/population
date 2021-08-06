#### FUNCTIONS TO CREATE, MODIFY AND DROP TABLES
#
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2020-12

### FUNCTION TO CREATE DATABASE CONNECTION
create_conn_f <-function(server = c("APDEStore", "hhsaw"),
                         prod = T,
                         interactive = F) {

  if (server == "hhsaw") {
    db_name <- "hhs_analytics_workspace"
  } else if (server == "inthealth") {
    db_name <- "inthealth_edw"
  }
  
  if (prod == T & server %in% c("hhsaw")) {
    server_name <- "tcp:kcitazrhpasqlprp16.azds.kingcounty.gov,1433"
  } else {
    server_name <- "tcp:kcitazrhpasqldev20.database.windows.net,1433"
  }
  
  if (server == "APDEStore") {
    conn <- DBI::dbConnect(odbc::odbc(), "PH_APDEStore51")
  } else if (interactive == F) {
    conn <- DBI::dbConnect(odbc::odbc(),
                           driver = "ODBC Driver 17 for SQL Server",
                           server = server_name,
                           database = db_name,
                           uid = keyring::key_list("hhsaw_dev")[["username"]],
                           pwd = keyring::key_get("hhsaw_dev", keyring::key_list("hhsaw_dev")[["username"]]),
                           Encrypt = "yes",
                           TrustServerCertificate = "yes",
                           Authentication = "ActiveDirectoryPassword")
  } else if (interactive == T) {
    conn <- DBI::dbConnect(odbc::odbc(),
                           driver = "ODBC Driver 17 for SQL Server",
                           server = server_name,
                           database = db_name,
                           uid = keyring::key_list("hhsaw_dev")[["username"]],
                           Encrypt = "yes",
                           TrustServerCertificate = "yes",
                           Authentication = "ActiveDirectoryInteractive")
  }
  
  return(conn)
}

### FUNCTION TO CREATE TABLE BASED ON DATAFRAME OF VARS
create_table_f <- function(
  conn,
  schema,
  table,
  vars,
  overwrite = T) {
  #### IF OVERWRITE TRUE THEN DROP TABLE ####
  if (overwrite == T) {
    drop_table_f(conn, schema, table)
  }
  #### CREATE TABLE ####  
  create_code <- glue::glue_sql(
    "CREATE TABLE {`schema`}.{`table`} (
    {DBI::SQL(glue::glue_collapse(glue::glue_sql('{`names(vars)`} {DBI::SQL(vars)}', 
    .con = conn), sep = ', \n'))}
    )", 
    .con = conn)
  DBI::dbExecute(conn, create_code)
}

### FUNCTION TO ALTER TABLE (ADD COLUMNS) BASED ON DATAFRAME OF VARS
alter_table_f <- function(
  conn,
  schema,
  table,
  vars) {
  #### VARIABLES ####
  object <- paste0(schema, ".", table)
  #### ALTER TABLE ####  
  alter_code <- glue::glue_sql(
    "IF NOT EXISTS(SELECT 1 FROM sys.columns WHERE Name = {names(vars)[1]} AND OBJECT_ID = OBJECT_ID({object}))
      BEGIN
        ALTER TABLE {`schema`}.{`table`} 
        ADD {DBI::SQL(glue::glue_collapse(glue::glue_sql('{`names(vars)`} {DBI::SQL(vars)}', 
        .con = conn), sep = ', \n'))}
      END", 
    .con = conn)
  DBI::dbExecute(conn, alter_code)
}

### FUNCTION TO ALTER TABLE IF TABLE EXISTS
drop_table_f <- function(
  conn,
  schema,
  table) {
  #### DROP TABLE ####  
  if (DBI::dbExistsTable(conn, DBI::Id(schema = schema, table = table))) {
    DBI::dbExecute(conn, 
                   glue::glue_sql("DROP TABLE {`schema`}.{`table`}",
                                  .con = conn))
  }
}

### FUNCTION TO RETURN LIST OF COLUMNS FROM TABLE
get_table_cols_f <- function(
  conn,
  schema,
  table) {
  ### RETURNS LIST OF COLUMNS FROM SPECIFIC TABLE
  object <- paste0(schema, ".", table)
  columns <- DBI::dbGetQuery(conn,
                             glue::glue_sql(
                               "SELECT c.name AS 'col' FROM sys.columns c 
                               WHERE c.object_id = OBJECT_ID({object})",
                               .con = conn))
  return(columns)
}

### FUNCTION TO ADD INDEX TO TABLE
add_index_f <- function(conn, 
                        schema,
                        table,
                        index_name) {
  #### ADD INDEX ####
  message(glue::glue("Adding index ({index_name}) to {schema}.{table}"))
  dbGetQuery(conn, 
             glue::glue_sql("CREATE CLUSTERED COLUMNSTORE INDEX {`index_name`} 
                            ON {`schema`}.{`table`}", 
                            .con = conn))
}

### FUNCTION TO DROP INDEX FROM TABLE
drop_index_f <- function(conn, 
                        schema,
                        table,
                        index_name) {
  #### REMOVE EXISTING INDEX ID DESIRED ####
  message("Removing existing clustered columnstore index")
  dbGetQuery(conn, 
             glue::glue_sql("DROP INDEX {`index_name`} ON {`schema`}.{`table`}",
                            .con = conn))
}