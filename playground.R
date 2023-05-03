library(digest)

# Create sample data frame
df <- data.frame(col1 = c("apple", "banana", "cherry"), 
                 col2 = c("red", "yellow", "red"))

# Combine values from col1 and col2 into a single string for each row
df$combined <- paste(df$col1, df$col2, sep = "_")

# Hash the combined string using sha256 algorithm
df$hash <- digest(df$combined)

# View the resulting data frame
df