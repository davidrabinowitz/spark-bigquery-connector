from pyspark.sql import SparkSession
from pyspark.sql.types import *
import argparse
import json
import datetime

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--mode", default="append")
    parser.add_argument("--method", default="direct")
    parser.add_argument("--format", default="parquet")
    parser.add_argument("--write_at_least_once", default="false")
    parser.add_argument("--partition_overwrite_mode", default="DYNAMIC")
    
    args = parser.parse_args()
    spark = SparkSession.builder.appName("WriteDynamicTimestamp").getOrCreate()
    
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("order_date_time", TimestampType(), True)
    ])
    data = [
        (10, datetime.datetime(2023, 9, 28, 10, 15, 0)),
        (20, datetime.datetime(2023, 9, 30, 12, 0, 0))
    ]
    df = spark.createDataFrame(data, schema)
    
    writer = df.write.format("bigquery") \
        .mode(args.mode) \
        .option("table", args.table) \
        .option("temporaryGcsBucket", args.bucket) \
        .option("intermediateFormat", args.format) \
        .option("writeMethod", args.method) \
        .option("writeAtLeastOnce", args.write_at_least_once) \
        .option("spark.sql.sources.partitionOverwriteMode", args.partition_overwrite_mode)
        
    writer.save()
    
    print("===BEGIN OUTPUT===")
    print(json.dumps({"status": "success"}))
    print("===END OUTPUT===")
