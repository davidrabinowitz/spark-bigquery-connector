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
    spark = SparkSession.builder.appName("ReadCache").getOrCreate()
    
    # 1. Write data (assume table created or use write_basic logic)
    # The java test uses `readAllTypesTable()` then writes it.
    # I can just write simple data.
    
    schema = StructType([StructField("str", StringType(), True)])
    df_write = spark.createDataFrame([("foo",)], schema)
    df_write.write.format("bigquery") \
        .mode("overwrite") \
        .option("table", args.table) \
        .option("temporaryGcsBucket", args.bucket) \
        .option("writeMethod", args.method) \
        .save()
        
    # 2. Read and cache
    df = spark.read.format("bigquery").option("table", args.table).load().cache()
    
    head1 = df.first()
    head2 = df.first()
    
    print("===BEGIN OUTPUT===")
    print(json.dumps({"head1": head1[0], "head2": head2[0]}))
    print("===END OUTPUT===")

