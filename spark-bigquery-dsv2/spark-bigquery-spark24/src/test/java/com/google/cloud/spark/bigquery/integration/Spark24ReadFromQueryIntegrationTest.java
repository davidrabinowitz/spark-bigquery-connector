package com.google.cloud.spark.bigquery.integration;

import org.apache.spark.sql.SparkSession;

public class Spark24ReadFromQueryIntegrationTest  extends ReadFromQueryIntegrationTestBase {

  protected Spark24ReadFromQueryIntegrationTest(SparkSession spark,
      String testDataset) {
    super(spark, testDataset);
  }
}
