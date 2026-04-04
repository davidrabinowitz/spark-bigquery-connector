package com.google.cloud.spark.bigquery.integration;

import com.google.auth.oauth2.GoogleCredentials;
import java.util.Date;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class IcebergSandbox {

  Date refreshTime;

  @Test
  public void testIceberg() throws Exception {
    try (SparkSession spark = createSparkSession()) {
      spark.sql("SHOW DATABASES;").show();
      spark.sql("CREATE DATABASE davidrab_test_" + System.currentTimeMillis());
      long sleepSecs = (refreshTime.getTime() - System.currentTimeMillis()) + 10;
      Thread.sleep(sleepSecs * 1000);
      spark.sql("CREATE DATABASE davidrab_test_" + System.currentTimeMillis());
    }
  }

  SparkSession createSparkSession() throws Exception {
    String catalog_name = "iceberg";
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    credentials.refreshIfExpired();
    refreshTime = credentials.getAccessToken().getExpirationTime();
    System.out.println("Token refresh at [" + refreshTime + "]");
    return SparkSession.builder()
        .appName("iceberg test")
        .master("local[*]")
        .config("spark.sql.defaultCatalog", catalog_name)
        .config(
            String.format("spark.sql.catalog.%s", catalog_name),
            "org.apache.iceberg.spark.SparkCatalog")
        .config(String.format("spark.sql.catalog.%s.type", catalog_name), "rest")
        .config(
            String.format("spark.sql.catalog.%s.uri", catalog_name),
            "https://test-biglake.sandbox.googleapis.com/iceberg/v1alpha/restcatalog")
        .config(
            String.format("spark.sql.catalog.%s.warehouse", catalog_name), "gs://davidrab-iceberg")
        .config(
            String.format("spark.sql.catalog.%s.token", catalog_name),
            credentials.getAccessToken().getTokenValue())
        .config(
            String.format("spark.sql.catalog.%s.oauth2-server-uri", catalog_name),
            "https://oauth2.googleapis.com/token")
        .config(
            String.format("spark.sql.catalog.%s.header.x-goog-user-project", catalog_name),
            "google.com:hadoop-cloud-dev")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate();
  }
}
