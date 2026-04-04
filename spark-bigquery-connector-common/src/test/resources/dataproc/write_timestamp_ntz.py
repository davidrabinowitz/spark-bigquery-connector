from pyspark.sql import SparkSession
from pyspark.sql.types import *
import argparse
import json
import datetime

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--mode", default="overwrite")
    parser.add_argument("--format", default="parquet")
    parser.add_argument("--method", default="direct")
    
    args = parser.parse_args()
    spark = SparkSession.builder.appName("WriteTimestampNTZ").getOrCreate()
    
    try:
        from pyspark.sql.types import TimestampNTZType
        ntz_type = TimestampNTZType()
    except ImportError:
        print("===BEGIN OUTPUT===")
        print(json.dumps({"status": "skipped", "reason": "TimestampNTZType not available"}))
        print("===END OUTPUT===")
        exit(0)

    schema = StructType([StructField("foo", ntz_type, True)])
    data = [(datetime.datetime(2023, 9, 1, 12, 23, 34, 268543),)]
    df = spark.createDataFrame(data, schema)
    
    df.write.format("bigquery") \
        .mode(args.mode) \
        .option("table", args.table) \
        .option("temporaryGcsBucket", args.bucket) \
        .option("intermediateFormat", args.format) \
        .option("writeMethod", args.method) \
        .save()
        
    print("===BEGIN OUTPUT===")
    print(json.dumps({"status": "success"}))
    print("===END OUTPUT===")
