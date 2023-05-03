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




