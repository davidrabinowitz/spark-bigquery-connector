from pyspark.sql import SparkSession
from pyspark.sql.types import *
import argparse
import json

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table_prefix", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--method", default="direct")
    
    args = parser.parse_args()
    spark = SparkSession.builder.appName("WriteWithDescription").getOrCreate()
    
    test_description = "test description"
    test_comment = "test comment"
    
    s0 = StructType([StructField("c1", IntegerType(), True, {"description": test_description})])
    s1 = StructType([StructField("c1", IntegerType(), True, {"comment": test_comment})])
    s2 = StructType([StructField("c1", IntegerType(), True, {"description": test_description, "comment": test_comment})])
    s3 = StructType([StructField("c1", IntegerType(), True)])
    
    schemas = [s0, s1, s2, s3]
    data = [(100,), (200,)]
    
    for i, schema in enumerate(schemas):
        df = spark.createDataFrame(data, schema)
        df.write.format("bigquery") \
            .mode("overwrite") \
            .option("table", f"{args.table_prefix}_{i}") \
            .option("temporaryGcsBucket", args.bucket) \
            .option("intermediateFormat", "parquet") \
            .option("writeMethod", args.method) \
            .save()
            
    results = []
    for i in range(4):
        read_df = spark.read.format("bigquery").load(f"{args.table_prefix}_{i}")
        field = read_df.schema.fields[0]
        desc = field.metadata.get("description")
        comment = field.metadata.get("comment")
        results.append({"description": desc, "comment": comment})
        
    print("===BEGIN OUTPUT===")
    print(json.dumps(results))
    print("===END OUTPUT===")
