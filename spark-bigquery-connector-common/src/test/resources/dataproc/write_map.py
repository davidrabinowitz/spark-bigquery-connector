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
    parser.add_argument("--format", default="avro")
    parser.add_argument("--write_at_least_once", default="false")
    
    args = parser.parse_args()
    spark = SparkSession.builder.appName("WriteMap").getOrCreate()
    
    schema = StructType([StructField("mf", MapType(StringType(), LongType()), False)])
    data = [({"a":1, "b":2},), ({"c":3},)]
    df = spark.createDataFrame(data, schema)
    
    writer = df.write.format("bigquery") \
        .mode(args.mode) \
        .option("table", args.table) \
        .option("temporaryGcsBucket", args.bucket) \
        .option("intermediateFormat", args.format) \
        .option("writeMethod", args.method) \
        .option("writeAtLeastOnce", args.write_at_least_once)
        
    writer.save()
    
    print("===BEGIN OUTPUT===")
    print(json.dumps({"status": "success"}))
    print("===END OUTPUT===")
