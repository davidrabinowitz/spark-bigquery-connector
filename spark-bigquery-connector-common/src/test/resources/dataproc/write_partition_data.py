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
    parser.add_argument("--method", default="indirect")
    parser.add_argument("--format", default="parquet")
    parser.add_argument("--write_at_least_once", default="false")
    # Partition options
    parser.add_argument("--partition_field")
    parser.add_argument("--partition_type")
    parser.add_argument("--partition_require_filter")
    
    args = parser.parse_args()
    spark = SparkSession.builder.appName("WritePartitionData").getOrCreate()
    
    schema = StructType([
        StructField("str", StringType(), True),
        StructField("ts", TimestampType(), True)
    ])
    data = [
        ("a", datetime.datetime(2020, 1, 1, 1, 1, 1)),
        ("b", datetime.datetime(2020, 1, 2, 2, 2, 2)),
        ("c", datetime.datetime(2020, 1, 3, 3, 3, 3))
    ]
    df = spark.createDataFrame(data, schema)
    
    writer = df.write.format("bigquery") \
        .mode(args.mode) \
        .option("table", args.table) \
        .option("temporaryGcsBucket", args.bucket) \
        .option("intermediateFormat", args.format) \
        .option("writeMethod", args.method) \
        .option("writeAtLeastOnce", args.write_at_least_once)
        
    if args.partition_field:
        writer = writer.option("partitionField", args.partition_field)
    if args.partition_type:
        writer = writer.option("partitionType", args.partition_type)
    if args.partition_require_filter:
        writer = writer.option("partitionRequireFilter", args.partition_require_filter)
        
    writer.save() 
    
    print("===BEGIN OUTPUT===")
    print(json.dumps({"status": "success"}))
    print("===END OUTPUT===")
