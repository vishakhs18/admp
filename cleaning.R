library(dplyr)
library(tidyr)
library(stringi)

pollutant_pre_process <- function(df) {
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
  
  return(df)
}

no2_pre_processed <- pollutant_pre_process(no2)
pm10_pre_processed <- pollutant_pre_process(pm10)
pm2_5_pre_processed <- pollutant_pre_process(pm2_5)


# Moving filtered out records to audit table
audit_no2_vol_negative <- filter(no2_pre_processed, !Particulate_matter >= 0)

# Filtering out rows where pollutant_vol is negative
no2_cleaned <- filter(no2_pre_processed, Particulate_matter >= 0)

# Moving filtered out records to audit table
audit_no2_no_data <- filter(no2_cleaned, Particulate_matter == "No data")

# Filtering out rows where pollutant_vol is "No data"
no2_cleaned <- filter(no2_cleaned, !Particulate_matter == "No data")

# Filtering out rows which has invalid date
no2_cleaned <- subset(no2_cleaned, date != "End")

# Moving filtered out records to audit table
audit_no2_is_na <- filter(no2_cleaned, is.na(Particulate_matter))

# Filtering out rows which are NA
no2_df <- filter(no2_cleaned, !is.na(Particulate_matter))


###########

# Moving filtered out records to audit table
audit_pm10_vol_negative <- filter(pm10_pre_processed, !Particulate_matter >= 0)

# Filtering out rows where pollutant_vol is negative
pm10_cleaned <- filter(pm10_pre_processed, Particulate_matter >= 0)

# Moving filtered out records to audit table
audit_pm10_no_data <- filter(pm10_cleaned, Particulate_matter == "No data")

# Filtering out rows where pollutant_vol is "No data"
pm10_cleaned <- filter(pm10_cleaned, !Particulate_matter == "No data")

# Filtering out rows which has invalid date
pm10_cleaned <- subset(pm10_cleaned, date != "End")

# Moving filtered out records to audit table
audit_pm10_is_na <- filter(pm10_cleaned, is.na(Particulate_matter))

# Filtering out rows which are NA
pm10_df <- filter(pm10_cleaned, !is.na(Particulate_matter))

############
# Moving filtered out records to audit table
audit_pm2_5_vol_negative <- filter(pm2_5_pre_processed, !Particulate_matter >= 0)

# Filtering out rows where pollutant_vol is negative
pm2_5_cleaned <- filter(pm2_5_pre_processed, Particulate_matter >= 0)

# Moving filtered out records to audit table
audit_pm2_5_no_data <- filter(pm2_5_cleaned, Particulate_matter == "No data")

# Filtering out rows where pollutant_vol is "No data"
pm2_5_cleaned <- filter(pm2_5_cleaned, !Particulate_matter == "No data")

# Filtering out rows which has invalid date
pm2_5_cleaned <- subset(pm2_5_cleaned, date != "End")

# Moving filtered out records to audit table
audit_pm2_5_is_na <- filter(pm2_5_cleaned, is.na(Particulate_matter))

# Filtering out rows which are NA
pm2_5_df <- filter(pm2_5_cleaned, !is.na(Particulate_matter))



################################### TRAFFIC


traffic_pre_process <- function(df){
  # Selecting required columns
  traffic_df = subset(traffic, select = c(Year,Count_date,Local_authority_code,
                                          Local_authority_name, 
                                          Two_wheeled_motor_vehicles:LGVs, All_HGVs))
  
  # Selecting the required range of data
  traffic_df = subset(traffic_df, Year > 2017)
  
  # Extracting month from date
  traffic_df$month <- stri_sub(traffic_df$Count_date, from = 6, to = 7)
  
  # Converting wide data to long data
  traffic_df <- traffic_df %>% gather(vehicle_type, count, Two_wheeled_motor_vehicles:All_HGVs)
}

traffic_pre_processed <- traffic_pre_process(traffic)

audit_traf_is_na <- filter(traffic_pre_processed,is.na(count))

# Filtering out rows which are NA
traffic_df <- filter(traffic_pre_processed, !is.na(count))

traffic_df <- traffic_df %>%
  group_by(Year, Count_date, Local_authority_name, vehicle_type) %>%
  summarize(count = sum(as.numeric(count)))





