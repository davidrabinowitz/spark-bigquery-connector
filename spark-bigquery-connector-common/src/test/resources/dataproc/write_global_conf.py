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
    
    spark = SparkSession.builder.appName("WriteGlobalConf") \
        .config("bigQueryTableLabel.foo", "bar") \
        .getOrCreate()
        
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("friends", ArrayType(
            StructType([
                StructField("age", IntegerType(), True),
                StructField("links", ArrayType(
                    StructType([
                        StructField("uri", StringType(), True)
                    ])
                , True))
            ])
        , True))
    ])
    
    df = spark.createDataFrame([
        ("Abc", [ (10, [("www.abc.com",)]) ])
    ], schema=schema)
    
    df.write.format("bigquery") \
        .option("temporaryGcsBucket", args.bucket) \
        .option("table", args.table) \
        .option("writeMethod", args.method) \
        .save()
        
    print("===BEGIN OUTPUT===")
    print(json.dumps({"status": "success"}))
    print("===END OUTPUT===")

