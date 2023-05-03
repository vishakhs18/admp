#install.packages("HiveR")
#install.packages("Rcpp")
#install.packages("odbc")

library(dplyr)
library("DBI")
library("HiveR")
library("odbc")
library("Rcpp")
library("digest")

con <- dbConnect(odbc(),
                      "Sample Cloudera Hive DSN",
                 database="admp_final")


# Writing to dim_vehicle_type
table_name <- "dim_vehicle_type"
vehicle_type <- unique(traffic_df$vehicle_type)
vehicle_type_id <- as.character(unlist(sapply(vehicle_type, digest, simplify = FALSE)))
df <- data.frame(vehicle_type_id, vehicle_type)
write.csv(df,"tables/dim_vehicle_type.csv", row.names = FALSE)

dbWriteTable(con, table_name, df, overwrite = TRUE)

# Writing to dim_local_auth
table_name <- "dim_local_auth"
column_names <- "local_auth_id, local_auth_name"
local_auth_name <- unique(traffic_df$Local_authority_name)
local_auth_id <- as.character(unlist(sapply(local_auth_name, digest, simplify = FALSE)))
df <- data.frame(local_auth_id, local_auth_name)
write.csv(df,"tables/dim_local_auth.csv", row.names = FALSE)

dbWriteTable(con, table_name, df, overwrite = TRUE)

# Writing to dim_pollnt_type
table_name <- "dim_pollnt_type"
column_names <- "pollnt_type_id, pollnt_type"
pollnt_type <- c("PM10", "PM2.5","NO2")
pollnt_type_id <- as.character(unlist(sapply(pollnt_type, digest, simplify = FALSE)))
df <- data.frame(pollnt_type_id, pollnt_type)
write.csv(df,"tables/dim_pollnt_type.csv", row.names = FALSE)

dbWriteTable(con, table_name, df, overwrite = TRUE)

# Writing to dim_year
table_name <- "dim_year"
column_names <- "year_id, year"
year <- as.character(unique(traffic_df$Year))
year_id <- as.character(unlist(sapply(year, digest, simplify = FALSE)))
df <- data.frame(year_id, year)
write.csv(df,"tables/dim_year.csv", row.names = FALSE)

dbWriteTable(con, table_name, df, overwrite = TRUE)

# Writing to dim_month
table_name <- "dim_month"
column_names <- "month_id, year_id, month_no, season_type"

df <- select(fact2_df, month_id, year_id, month, season)
df <- distinct(df, month_id, year_id, month, season)
write.csv(df,"tables/dim_month.csv", row.names = FALSE)

dbWriteTable(con, table_name, df, overwrite = TRUE)

# Writing to dim_local_auth
table_name <- "dim_local_auth"
df <- distinct(pollnt_df, local_auth_id, Local_authority_name)
df <- merge(df, traffic, by = "Local_authority_name")
df <- select(df, local_auth_id, Local_authority_name, Latitude, Longitude)

df <- df %>% 
  group_by(local_auth_id, Local_authority_name) %>%
  summarise(Latitude = min(Latitude), min(Longitude))

write.csv(df,"tables/dim_local_auth.csv", row.names = FALSE)


dbWriteTable(con, table_name, df, overwrite = TRUE)


# Writing to dim_day_type
table_name <- "dim_day_type"
month_id <- pollnt_df$month_id
dates <- as.Date(substring(pollnt_df$date,1,10))
day_type <- format(dates, "%u")

df <- data.frame(month_id, day_type)
df$day_type <- ifelse(df$day_type %in% 1:5, "Weekday", "Weekend")
df$day_type_id <- paste(df$month_id,df$day_type,sep="")
df$day_type_id <- as.character(unlist(sapply(df$day_type_id, digest, simplify = FALSE)))
pollnt_df$day_type_id <- df$day_type_id

df <- distinct(df, day_type_id, month_id, day_type)
write.csv(df,"tables/dim_day_type.csv", row.names = FALSE)

dbWriteTable(con, table_name, df, overwrite = TRUE)

# Writing to dim_hour_type
table_name <- "dim_hour_type"
day_type_id <- pollnt_df$day_type_id
df <- data.frame(day_type_id)
df$hour <- substring(pollnt_df$time,1,2)


df$hour_type <- ifelse(df$hour %in% c(08,09,10,17,18,19), "peak", "off-peak")
df$hour_type_id <- paste(df$day_type_id,df$hour_type,sep="")
df$hour_type_id <- as.character(unlist(sapply(df$hour_type_id, digest, simplify = FALSE)))
pollnt_df$hour_type_id <- df$hour_type_id

df <- distinct(df, hour_type_id, day_type_id, hour_type)
write.csv(df,"tables/dim_hour_type.csv", row.names = FALSE)

dbWriteTable(con, table_name, df, overwrite = TRUE)

# Writing to fact_1
table_name <- "fact_1"
write.csv(fact1_df,"tables/fact_1.csv", row.names = FALSE)

dbWriteTable(con, table_name, fact1_df, overwrite = TRUE)


# Writing to fact_2
table_name <- "fact_2"
df <- select(fact2_df, pollnt_type, month_id, tot_vol, tot_count)
write.csv(df,"tables/fact_2.csv", row.names = FALSE)

dbWriteTable(con, table_name, df, overwrite = TRUE)

# Writing to fact_3
table_name <- "fact_3"
write.csv(fact3_df,"tables/fact_3.csv", row.names = FALSE)

dbWriteTable(con, table_name, fact3_df, overwrite = TRUE)

# Writing to fact_4
table_name <- "fact_4"
write.csv(fact4_df,"tables/fact_4.csv", row.names = FALSE)

dbWriteTable(con, table_name, fact4_df, overwrite = TRUE)

# Writing to fact_5
table_name <- "fact_5"
df <- select(fact5_df, pollnt_type, year_id, local_auth_id, vehicle_type_id, tot_count, tot_vol)
write.csv(df,"tables/fact_5.csv", row.names = FALSE)

dbWriteTable(con, table_name, fact5_df, overwrite = TRUE)