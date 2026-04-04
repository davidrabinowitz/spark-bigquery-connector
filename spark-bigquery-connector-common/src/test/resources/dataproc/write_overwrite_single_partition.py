from pyspark.sql import SparkSession
from pyspark.sql.types import *
import argparse
import json
import datetime

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--comment", default="")
    parser.add_argument("--method", default="direct")
    
    args = parser.parse_args()
    spark = SparkSession.builder.appName("WriteOverwriteSinglePartition").getOrCreate()
    
    date_field = StructField("the_date", DateType(), True)
    if args.comment:
        date_field = StructField("the_date", DateType(), True, {"comment": args.comment})
        
    schema = StructType([
        date_field,
        StructField("some_text", StringType(), True)
    ])
    
    data = [(datetime.date(2020, 7, 1), "baz")]
    df = spark.createDataFrame(data, schema)
    
    df.write.format("bigquery") \
        .option("temporaryGcsBucket", args.bucket) \
        .option("datePartition", "20200701") \
        .mode("overwrite") \
        .option("table", args.table) \
        .option("writeMethod", args.method) \
        .save()
        
    resultDF = spark.read.format("bigquery").load(args.table)
    rows = resultDF.collect()
    
    count = len(rows)
    bar_count = len([r for r in rows if r.some_text == "bar"])
    baz_count = len([r for r in rows if r.some_text == "baz"])
    
    print("===BEGIN OUTPUT===")
    print(json.dumps({
        "count": count,
        "bar_count": bar_count,
        "baz_count": baz_count
    }))
    print("===END OUTPUT===")
