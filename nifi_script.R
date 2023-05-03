library(readr)
traffic <- read_csv("C:\\Users\\LabStudent-55-706949\\Desktop\\datasets\\traffic_data.csv")

pm10 <- read_csv("C:\\Users\\LabStudent-55-706949\\Desktop\\datasets\\PM10.csv", skip=3)
pm2_5 <- read_csv("C:\\Users\\LabStudent-55-706949\\Desktop\\datasets\\PM2_5.csv", skip=3)
no2 <- read_csv("C:\\Users\\LabStudent-55-706949\\Desktop\\datasets\\no2.csv", skip=3)

lsoa <- read_csv("C:\\Users\\LabStudent-55-706949\\Desktop\\datasets\\lsoa_mapping.csv")


library(dplyr)
library(tidyr)
library(stringi)

clean_data <- function(df) {
  # Renaming columns
  df <- df %>% 
    rename(
      date = ...1,
      time = ...2
    )
  
  # Dropping unwanted columns in dataframe
  df <- select(df, -starts_with("..."))
  
  # Removing unwanted header in dataframe
  df <- df[-1,]
  
  # Converting wide data to long data
  df <- df %>% gather(Local_Area, Particulate_matter, "Aberdeen":"York Fishergate")
  
  df$Local_Area <- gsub(" ", ".", df$Local_Area)
  
  # Setting negative values of volume of pollutant as NA
  df$Particulate_matter <- ifelse(
    df$Particulate_matter < 0,
    NA, df$Particulate_matter)
  
  # Setting 'No data' values of volume of pollutant as NA
  df$Particulate_matter <- ifelse(
    df$Particulate_matter == 'No data',
    NA, df$Particulate_matter)
  
  # Filtering out rows which has invalid date
  df <- subset(df, date != "End")
  
  # Filtering out rows which are NA
  df <- subset(df, !is.na(Particulate_matter))
  
  # Checking completeness of the dataframe
  df[!complete.cases(df),]
  
  return(df)
  
}


pm10_df <- clean_data(pm10)
pm2_5_df <- clean_data(pm2_5)
no2_df <- clean_data(no2)


################################### TRAFFIC


# Selecting required columns
traffic_df = subset(traffic, select = c(Year,Count_date,Local_authority_code, Local_authority_name, Two_wheeled_motor_vehicles:LGVs, All_HGVs))

traffic_df = subset(traffic_df, Year > 2017)

traffic_df$month <- stri_sub(traffic_df$Count_date, from = 6, to = 7)

# Converting wide data to long data
traffic_df <- traffic_df %>% gather(vehicle_type, count, Two_wheeled_motor_vehicles:All_HGVs)

# Filtering out rows which are NA
traffic_df <- subset(traffic_df, !is.na(count))

traffic_df <- traffic_df %>%
  group_by(Year, Count_date, Local_authority_name, vehicle_type) %>%
  summarize(count = sum(as.numeric(count)))

# Checking completeness of the dataframe
traffic_df[!complete.cases(traffic_df),]


############
############
############

# install.packages("stringi")
library(stringr)
library(stringi)
library(digest)
lsoa$Local_Area <- gsub(" ", ".", lsoa$Local_Area)

merged_no2_lsoa <- merge(no2_df, lsoa, by.x = "Local_Area", by.y = "Local_Area")
merged_no2_lsoa$pollnt_type <- digest("NO2")
merged_no2_lsoa$year_id <- sapply(lapply(merged_no2_lsoa$date, function(x) digest(substr(x, 1, 4))), as.character)

merged_pm10_lsoa <- merge(pm10_df, lsoa, by.x = "Local_Area", by.y = "Local_Area")
merged_pm10_lsoa$pollnt_type <- digest("PM10")
# merged_pm10_lsoa$year_id <- as.character(unlist(sapply(substring(merged_pm10_lsoa$date, 1, 4), digest, simplify = FALSE)))
merged_pm10_lsoa$year_id <- sapply(lapply(merged_pm10_lsoa$date, function(x) digest(substr(x, 1, 4))), as.character)


merged_pm2_5_lsoa <- merge(pm2_5_df, lsoa, by.x = "Local_Area", by.y = "Local_Area")
merged_pm2_5_lsoa$pollnt_type <- digest("PM2.5")
# merged_pm2_5_lsoa$year_id <- as.character(unlist(sapply(substring(merged_pm2_5_lsoa$date, 1, 4), digest, simplify = FALSE)))
merged_pm2_5_lsoa$year_id <- sapply(lapply(merged_pm2_5_lsoa$date, function(x) digest(substr(x, 1, 4))), as.character)


pollnt_df <- rbind(merged_no2_lsoa, merged_pm10_lsoa, merged_pm2_5_lsoa)
pollnt_df$Year <- substring(pollnt_df$date, 1,4) 
pollnt_df$Month <- substring(pollnt_df$date, 6,7)
pollnt_df$month_id <- paste(pollnt_df$year_id, pollnt_df$Month,sep="")
pollnt_df$month_id <- as.character(unlist(sapply(pollnt_df$month_id, digest, simplify = FALSE)))

pollnt_df$Local_authority_name <- str_trim(pollnt_df$Local_authority_name)

pollnt_df$local_auth_id <- as.character(unlist(sapply(pollnt_df$Local_authority_name, digest, simplify = FALSE)))

traffic_df$month <- substring(traffic_df$Count_date, 6,7)
########
#######
#######
########

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

# Writing to dim_time
table_name <- "dim_time"
day_type_id <- pollnt_df$day_type_id
df <- data.frame(day_type_id)
df$hour <- substring(pollnt_df$time,1,2)


df$hour_type <- ifelse(df$hour %in% c(08,09,10,17,18,19), "peak", "off-peak")
df$hour_type_id <- paste(df$day_type_id,df$hour_type,sep="")
df$hour_type_id <- as.character(unlist(sapply(df$hour_type_id, digest, simplify = FALSE)))
pollnt_df$hour_type_id <- df$hour_type_id

df <- distinct(df, hour_type_id, day_type_id, hour_type)
write.csv(df,"tables/dim_time.csv", row.names = FALSE)

dbWriteTable(con, table_name, df, overwrite = TRUE)

############
##########
##########
#########

traffic_group_by_year <- traffic_df %>%
  group_by(Year) %>%
  summarize(tot_count = sum(as.numeric(count)))

fact1_df <- merge(traffic_group_by_year, pollnt_df, by.x = "Year", by.y = "Year")
fact1_df <- fact1_df[c("pollnt_type", "year_id","Particulate_matter","tot_count")]
fact1_df <- fact1_df %>%
  group_by(year_id,pollnt_type,tot_count) %>%
  summarize(tot_vol = sum(as.numeric(Particulate_matter)))

########################


traffic_group_by_year_month <- traffic_df %>%
  group_by(Year, month) %>%
  summarize(tot_count = sum(as.numeric(count)))

traffic_group_by_year_month$year_id <- as.character(unlist(sapply(as.character(traffic_group_by_year_month$Year), digest, simplify = FALSE)))
traffic_group_by_year_month$month_id <- paste(traffic_group_by_year_month$year_id, traffic_group_by_year_month$month,sep="")
traffic_group_by_year_month$month_id <- as.character(unlist(sapply(traffic_group_by_year_month$month_id, digest, simplify = FALSE)))

pollnt_df_group_by_month_id <- pollnt_df %>%
  group_by(month_id, pollnt_type) %>%
  summarize(tot_vol = sum(as.numeric(Particulate_matter)))



fact2_df <- merge(traffic_group_by_year_month, pollnt_df_group_by_month_id, by = "month_id")
fact2_df$season <- ifelse(fact2_df$month %in% c('03','04','05'), 'spring',
                          ifelse(fact2_df$month %in% c('06','07','08'), 'summer',
                                 ifelse(fact2_df$month %in% c('09','10','11'), 'autumn',
                                        'winter')))
##############################
fact3_df <- pollnt_df %>%
  group_by(pollnt_type, local_auth_id, hour_type_id) %>%
  summarize(tot_vol = sum(as.numeric(Particulate_matter)))
##############################
fact4_df <- pollnt_df %>%
  group_by(pollnt_type, local_auth_id, day_type_id) %>%
  summarize(tot_vol = sum(as.numeric(Particulate_matter)))
##############################
traffic_group_by_year_type <- traffic_df %>%
  group_by(Year, Local_authority_name, vehicle_type) %>%
  summarize(tot_count = sum(as.numeric(count)))

pollnt_by_type_year_area <- pollnt_df %>%
  group_by(pollnt_type, year_id, Year, local_auth_id, Local_authority_name) %>%
  summarize(tot_vol = sum(as.numeric(Particulate_matter)))

fact5_df <- merge(traffic_group_by_year_type, pollnt_by_type_year_area, by = c("Year", "Local_authority_name"))
fact5_df$vehicle_type_id <- as.character(unlist(sapply(fact5_df$vehicle_type, digest, simplify = FALSE)))

#################
##############
#############


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

dbWriteTable(con, table_name, fact3_df, overwrite = TRUE)

# Writing to fact_5
table_name <- "fact_5"
df <- select(fact5_df, pollnt_type, year_id, local_auth_id, vehicle_type_id, tot_count, tot_vol)
write.csv(df,"tables/fact_5.csv", row.names = FALSE)

dbWriteTable(con, table_name, fact5_df, overwrite = TRUE)






