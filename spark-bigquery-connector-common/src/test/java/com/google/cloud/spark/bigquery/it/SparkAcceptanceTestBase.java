package com.google.cloud.spark.bigquery.it;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkAcceptanceTestBase {

  protected static final String LARGE_TABLE = "bigquery-public-data.samples.natality";
  protected static final String LARGE_TABLE_FIELD = "is_male";
  protected static final long LARGE_TABLE_NUM_ROWS = 33271914L;
  protected static final String NON_EXISTENT_TABLE = "non-existent.non-existent.non-existent";
  protected static final String ALL_TYPES_TABLE_NAME = "all_types";

  protected String datasource;
  protected SparkSession spark;
  protected BigQuery bq;
  protected String testDataset = "spark_bigquery_it_" + System.currentTimeMillis();
  protected String testTable;
  protected String fullTableName;

  protected SparkAcceptanceTestBase(String datasource) {
    this.datasource = datasource;
    this.bq = BigQueryOptions.getDefaultInstance().getService();
  }

  protected void setup() {
    testTable = "test_" + System.nanoTime();
    fullTableName = testDataset + "." + testTable;
  }

  Dataset<Row> readAllTypesTable() {
    return spark
        .read()
        .format(datasource)
        .option("dataset", testDataset)
        .option("table", ALL_TYPES_TABLE_NAME)
        .load();
  }
}
