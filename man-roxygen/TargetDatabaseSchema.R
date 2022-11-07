#' @param targetDatabaseSchema   (Optional) Schema name where output pathway tables would reside. This is also known as
#'                               as resultsDatabaseSChema. If not specified, scratch schema will be used. The output may not
#'                               persist in the database after disconnection. Note that for SQL Server,
#'                               this should include both the database and schema name, for example
#'                               'scratch.dbo'. 
