from pyspark.sql import SparkSession
from pyspark.sql.types import *
import argparse
import json

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--method", default="direct")
    
    args = parser.parse_args()
    spark = SparkSession.builder.appName("WriteEmpty").getOrCreate()
    
    schema = StructType([StructField("uri", StringType(), True)])
    df = spark.createDataFrame([], schema)
    
    df.write.format("bigquery") \
        .mode("append") \
        .option("table", args.table) \
        .option("temporaryGcsBucket", args.bucket) \
        .option("writeMethod", args.method) \
        .save()
        
    print("===BEGIN OUTPUT===")
    print(json.dumps({"status": "success"}))
    print("===END OUTPUT===")
