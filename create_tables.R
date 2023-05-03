library(DBI)
library("odbc")
con <- dbConnect(odbc(),
                 "Sample Cloudera Hive DSN",
                 database="admp_final")



query <- "CREATE TABLE IF NOT EXISTS dim_local_auth (local_auth_id STRING, local_auth_name STRING)"
result <- dbSendQuery(con, query)
dbClearResult(result)

query <- "CREATE TABLE IF NOT EXISTS dim_vehicle_type (vehicle_type_id STRING, vehicle_type STRING)"
result <- dbSendQuery(con, query)
dbClearResult(result)

query <- "CREATE TABLE IF NOT EXISTS dim_pollnt_type (pollnt_type_id STRING, pollnt_type STRING)"
result <- dbSendQuery(con, query)
dbClearResult(result)

query <- "CREATE TABLE IF NOT EXISTS dim_year (year_id STRING, year STRING)"
result <- dbSendQuery(con, query)
dbClearResult(result)

query <- "CREATE TABLE IF NOT EXISTS dim_month (month_id STRING, year_id STRING, month_no STRING, season_type STRING)"
result <- dbSendQuery(con, query)
dbClearResult(result)

query <- "CREATE TABLE IF NOT EXISTS dim_time (time_id STRING, day_id STRING, hour STRING, hour_type STRING)"
result <- dbSendQuery(con, query)
dbClearResult(result)

query <- "CREATE TABLE IF NOT EXISTS dim_day_type (day_type_id STRING, month_id STRING, day_type STRING)"
result <- dbSendQuery(con, query)
dbClearResult(result)

query <- "DROP TABLE IF EXISTS FACT_5"
result <- dbSendQuery(con, query)
dbClearResult(result)

query <- "CREATE TABLE IF NOT EXISTS fact_1 (
  pollnt_type_id STRING, 
  year_id STRING, 
  total_vol_of_pollnt int,
  total_count_of_traffic int)"
result <- dbSendQuery(con, query)
dbClearResult(result)

query <- "CREATE TABLE IF NOT EXISTS fact_2 (
  pollnt_type_id STRING, 
  month_id STRING, 
  avg_vol_of_pollnt INT,
  total_count_of_traffic int)"
result <- dbSendQuery(con, query)
dbClearResult(result)

query <- "CREATE TABLE IF NOT EXISTS fact_3 (
  pollnt_type_id STRING, 
  local_auth_id STRING,
  time_id STRING,
  total_vol_of_pollnt INT)"
result <- dbSendQuery(con, query)
dbClearResult(result)

query <- "CREATE TABLE IF NOT EXISTS fact_4 (
  pollnt_type_id STRING, 
  local_auth_id STRING,
  day_type_id STRING,
  total_vol_of_pollnt INT)"
result <- dbSendQuery(con, query)
dbClearResult(result)

query <- "CREATE TABLE IF NOT EXISTS fact_5 (
  pollnt_type_id STRING, 
  local_auth_id STRING,
  vehicle_type_id STRING,
  total_vol_of_pollnt INT)"
result <- dbSendQuery(con, query)
dbClearResult(result)


