# install.packages("stringi")
library(stringr)
library(stringi)
library(digest)


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








