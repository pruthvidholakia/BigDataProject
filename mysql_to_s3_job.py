import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from datetime import datetime

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# MySQL Configuration
jdbc_url = "jdbc:mysql://j7ozrq.h.filess.io:3307/olistProject_whileroute"
user = "olistProject_whileroute"
password = "1ee0b43eb6d2eec6ce15f5b94e261517f9b18305"
table = "olist_order_payments"

# S3 Output Path
bucket = "olist-data-storage-account"
partition_date = datetime.now().strftime('%Y-%m-%d')
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
output_path = f"s3://{bucket}/bronze/olist_order_payments_dataset/{partition_date}/"

# Read from MySQL via JDBC
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", table) \
    .option("user", user) \
    .option("password", password) \
    .load()

# Write to S3 as CSV
df.write.option("header", True).mode("overwrite").csv(output_path)

print(f"Exported data to: {output_path}")
