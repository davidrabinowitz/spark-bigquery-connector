/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.integration;

import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

public class Spark35DirectWriteIntegrationTest extends WriteIntegrationTestBase {

  public Spark35DirectWriteIntegrationTest() {
    super(SparkBigQueryConfig.WriteMethod.DIRECT, DataTypes.TimestampNTZType);
  }

  // tests from superclass

  @Test
  public void foo() throws Exception {
    SparkSession sparkSession =
        createSparkSession(
            ImmutableMap.of(
                "spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener"));

    // create source table
    String sourceTable = testTable + "_source";
    IntegrationTestUtils.runQuery(
        String.format("CREATE TABLE `%s.%s` COPY `bigquery-public-data.samples.shakespeare`"));

    // read
    Dataset<Row> df =
        sparkSession
            .read()
            .format("bigquery")
            .option("Dataset", testDataset.toString())
            .option("table", sourceTable)
            .load();

    // write
    // table is "fullTableName"
    writeToBigQuery(df, SaveMode.ErrorIfExists, "AVRO");

    // Analyze OpenLineage
  }
}
