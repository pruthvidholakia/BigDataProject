import requests
import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd
from awsglue.context import GlueContext
from pyspark.context import SparkContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

s3 = boto3.client("s3")
bucket = "olist-data-storage-account"
prefix = "bronze"

json_url = "https://raw.githubusercontent.com/pruthvidholakia/BigDataProject/main/brazilian-ecommerce-analysis/olist_datasets.json"
base_url = "https://raw.githubusercontent.com/pruthvidholakia/"
date_partition = datetime.now().strftime("%Y-%m-%d")

datasets = json.loads(requests.get(json_url).text)

for ds in datasets:
    relative_path = ds["csv_relative_url"]
    file_name = ds["file_name"]
    dataset_folder = file_name.replace(".csv", "")
    full_url = base_url + relative_path

    print(f"Downloading: {full_url}")
    file_response = requests.get(full_url)
    if file_response.status_code != 200:
        print(f"Failed to fetch: {full_url}")
        continue

    pd_df = pd.read_csv(StringIO(file_response.text))
    spark_df = spark.createDataFrame(pd_df)

    output_path = f"s3://{bucket}/{prefix}/{dataset_folder}/{date_partition}/"
    spark_df.coalesce(1).write.option("header", True).mode("overwrite").csv(output_path)

print("All datasets uploaded (default Spark part files, no cost)")
