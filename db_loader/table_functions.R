#### FUNCTIONS TO CREATE, MODIFY AND DROP TABLES
#
# Jeremy Whitehurst, PHSKC (APDE)
#
# 2020-12

create_table_f <- function(
  conn,
  config,
  overwrite = T) {
  
  #### VARIABLES ####
  schema <- config$schema_name
  table <- config$table_name
  
  #### OVERWRITE TABLE ####
  if (overwrite == T) {
    if (DBI::dbExistsTable(conn, DBI::Id(schema = schema, table = table))) {
      DBI::dbExecute(conn, 
                     glue::glue_sql("DROP TABLE {`schema`}.{`table`}",
                                    .con = conn))
    }
  }
  
  #### CREATE TABLE ####  
  create_code <- glue::glue_sql(
    "CREATE TABLE {`schema`}.{`table`} (
    {DBI::SQL(glue::glue_collapse(glue::glue_sql('{`names(config$vars)`} {DBI::SQL(config$vars)}', 
    .con = conn), sep = ', \n'))}
    )", 
    .con = conn)
  DBI::dbExecute(conn, create_code)
}

alter_table_f <- function(
  conn,
  config) {
  
  #### VARIABLES ####
  schema <- config$schema_name
  table <- config$table_name
  
  #### ALTER TABLE ####  
  alter_code <- glue::glue_sql(
    "ALTER TABLE {`schema`}.{`table`} 
    ADD {DBI::SQL(glue::glue_collapse(glue::glue_sql('{`names(config$vars_add)`} {DBI::SQL(config$vars_add)}', 
    .con = conn), sep = ', \n'))}", 
    .con = conn)
  DBI::dbExecute(conn, alter_code)
}

drop_table_f <- function(
  conn,
  config) {
  
  #### VARIABLES ####
  schema <- config$schema_name
  table <- config$table_name
  
  #### ALTER TABLE ####  
  drop_code <- glue::glue_sql(
    "DROP TABLE {`schema`}.{`table`}", 
    .con = conn)
  DBI::dbExecute(conn, drop_code)
}