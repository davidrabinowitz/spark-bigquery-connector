from pyspark.sql import SparkSession
import argparse
import json

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--src_table", required=True)
    parser.add_argument("--dest_table", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--mode", default="append")
    parser.add_argument("--method", default="direct")
    parser.add_argument("--write_at_least_once", default="false")
    parser.add_argument("--enable_mode_check", default="true")
    
    args = parser.parse_args()
    spark = SparkSession.builder.appName("WriteDiffSchema").getOrCreate()
    
    df = spark.read.format("bigquery").option("table", args.src_table).load()
    
    df.write.format("bigquery") \
        .mode(args.mode) \
        .option("table", args.dest_table) \
        .option("temporaryGcsBucket", args.bucket) \
        .option("writeMethod", args.method) \
        .option("writeAtLeastOnce", args.write_at_least_once) \
        .option("enableModeCheckForSchemaFields", args.enable_mode_check) \
        .save()
        
    print("===BEGIN OUTPUT===")
    print(json.dumps({"status": "success"}))
    print("===END OUTPUT===")

