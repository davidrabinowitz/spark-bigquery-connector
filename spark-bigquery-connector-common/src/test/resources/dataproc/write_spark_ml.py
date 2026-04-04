from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml import Pipeline
import argparse
import json

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--method", default="direct")
    
    args = parser.parse_args()
    spark = SparkSession.builder.appName("WriteSparkML").getOrCreate()
    
    schema = StructType([
        StructField("Seqno", StringType(), True),
        StructField("date1", StringType(), True),
        StructField("num1", IntegerType(), True),
        StructField("num2", IntegerType(), True),
        StructField("amt1", DoubleType(), True)
    ])
    
    data = [
        ("1", "20230515", 12345, 5678, 1234.12345),
        ("2", "20230515", 14789, 25836, 1234.12345),
        ("3", "20230515", 54321, 98765, 1234.12345)
    ]
    df = spark.createDataFrame(data, schema)
    
    va = VectorAssembler(inputCols=["num1", "num2"], outputCol="features_vector")
    mm = MinMaxScaler(inputCol="features_vector", outputCol="features")
    pipeline = Pipeline(stages=[va, mm])
    
    model = pipeline.fit(df)
    df = model.transform(df)
    
    df.write.format("bigquery") \
        .option("table", args.table) \
        .option("temporaryGcsBucket", args.bucket) \
        .option("writeMethod", args.method) \
        .save()
        
    print("===BEGIN OUTPUT===")
    print(json.dumps({"status": "success"}))
    print("===END OUTPUT===")
