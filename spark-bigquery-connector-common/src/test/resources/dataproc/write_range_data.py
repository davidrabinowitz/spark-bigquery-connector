from pyspark.sql import SparkSession
from pyspark.sql.types import *
import argparse
import json

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--mode", default="append")
    parser.add_argument("--method", default="indirect")
    parser.add_argument("--format", default="parquet")
    parser.add_argument("--write_at_least_once", default="false")
    # Range partition options
    parser.add_argument("--partition_field")
    parser.add_argument("--partition_range_start")
    parser.add_argument("--partition_range_end")
    parser.add_argument("--partition_range_interval")
    parser.add_argument("--partition_require_filter")
    
    args = parser.parse_args()
    spark = SparkSession.builder.appName("WriteRangeData").getOrCreate()
    
    schema = StructType([
        StructField("str", StringType(), True),
        StructField("rng", LongType(), True)
    ])
    data = [("a", 1), ("b", 5), ("c", 11)]
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
    if args.partition_range_start:
        writer = writer.option("partitionRangeStart", args.partition_range_start)
    if args.partition_range_end:
        writer = writer.option("partitionRangeEnd", args.partition_range_end)
    if args.partition_range_interval:
        writer = writer.option("partitionRangeInterval", args.partition_range_interval)
    if args.partition_require_filter:
        writer = writer.option("partitionRequireFilter", args.partition_require_filter)
        
    writer.save()
    
    print("===BEGIN OUTPUT===")
    print(json.dumps({"status": "success"}))
    print("===END OUTPUT===")
