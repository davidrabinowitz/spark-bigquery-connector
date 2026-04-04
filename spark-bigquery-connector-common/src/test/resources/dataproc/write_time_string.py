from pyspark.sql import SparkSession
from pyspark.sql.types import *
import argparse
import json

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--mode", default="overwrite")
    parser.add_argument("--method", default="indirect")
    
    args = parser.parse_args()
    spark = SparkSession.builder.appName("WriteTimeString").getOrCreate()
    
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("wake_up_time", StringType(), True)
    ])
    data = [("abc", "10:00:00")]
    df = spark.createDataFrame(data, schema)
    
    df.write.format("bigquery") \
        .mode(args.mode) \
        .option("table", args.table) \
        .option("temporaryGcsBucket", args.bucket) \
        .option("writeMethod", args.method) \
        .save()
        
    print("===BEGIN OUTPUT===")
    print(json.dumps({"status": "success"}))
    print("===END OUTPUT===")

