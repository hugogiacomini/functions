from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, unix_timestamp, from_unixtime

# Assuming spark is a SparkSession
spark = SparkSession.builder.appName("example").getOrCreate()

# Your string
date_string = "2022.06.18 00:00:00"

# Convert string to timestamp
timestamp_col = unix_timestamp(date_string, "yyyy.MM.dd HH:mm:ss").cast("timestamp")

# Convert timestamp to date
date_col = to_date(from_unixtime(timestamp_col))

# Show the resulting date
date_col.show()