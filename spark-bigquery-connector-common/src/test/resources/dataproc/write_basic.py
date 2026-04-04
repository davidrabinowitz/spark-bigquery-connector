from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
import sys
import argparse
import json

def get_person_schema():
    return StructType([
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

def get_initial_data(spark):
    return spark.createDataFrame([
        ("Abc", [ (10, [("www.abc.com",)]) ]),
        ("Def", [ (12, [("www.def.com",)]) ])
    ], schema=get_person_schema())

def get_additional_data(spark):
    return spark.createDataFrame([
        ("Xyz", [ (10, [("www.xyz.com",)]) ]),
        ("Pqr", [ (12, [("www.pqr.com",)]) ])
    ], schema=get_person_schema())

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--mode", required=True)
    parser.add_argument("--data", required=True, choices=["initial", "additional"])
    parser.add_argument("--format", default="parquet")
    parser.add_argument("--method", default="indirect")
    parser.add_argument("--write_at_least_once", default="false")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--extra_options", default="") # key=value,key2=value2

    args = parser.parse_args()

    spark = SparkSession.builder.appName("WriteBasic").getOrCreate()

    if args.data == "initial":
        df = get_initial_data(spark)
    else:
        df = get_additional_data(spark)

    writer = df.write.format("bigquery") \
        .mode(args.mode) \
        .option("table", args.table) \
        .option("temporaryGcsBucket", args.bucket) \
        .option("intermediateFormat", args.format) \
        .option("writeMethod", args.method) \
        .option("writeAtLeastOnce", args.write_at_least_once)

    if args.extra_options:
        for option in args.extra_options.split(","):
            if "=" in option:
                key, value = option.split("=", 1)
                writer = writer.option(key, value)

    writer.save()

    print("===BEGIN OUTPUT===")
    print(json.dumps({"status": "success"}))
    print("===END OUTPUT===")
