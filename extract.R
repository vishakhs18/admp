#install.packages("readr")
library(readr)
traffic <- read_csv("C:\\Users\\LabStudent-55-706949\\Desktop\\datasets\\traffic_data.csv")

pm10 <- read_csv("C:\\Users\\LabStudent-55-706949\\Desktop\\datasets\\PM10.csv", skip=3)
pm2_5 <- read_csv("C:\\Users\\LabStudent-55-706949\\Desktop\\datasets\\PM2_5.csv", skip=3)
no2 <- read_csv("C:\\Users\\LabStudent-55-706949\\Desktop\\datasets\\no2.csv", skip=3)

lsoa <- read_csv("C:\\Users\\LabStudent-55-706949\\Desktop\\datasets\\lsoa_mapping.csv")


