package com.google.cloud.spark.bigquery.it;

import static com.google.common.truth.Truth.assertThat;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

public class WriteSupportAcceptanceTestBase extends SparkAcceptanceTestBase {

  private static final String LIBRARIES_PROJECTS_TABLE="bigquery-public-data.libraries_io.projects";
  private static final String PARQUET = "parquet";
  protected static final String TEMPORARY_GCS_BUCKET =
      IntegrationTestUtils.safeGetEnv("TEMPORARY_GCS_BUCKET");

  private Dataset<Row> initialData;
  private Dataset<Row> additonalData;
  protected String fullTableNamePartitioned;

  public WriteSupportAcceptanceTestBase(String datasource) {
    super(datasource);
    this.initialData =
        spark.createDataFrame(
            asList(
                new Person("Abc", asList(new Friend(10, asList(new Link("www.abc.com"))))),
                new Person("Def", asList(new Friend(12, asList(new Link("www.def.com")))))),
            Person.class);
    this.additonalData =
        spark.createDataFrame(
            asList(
                new Person("Pqr", asList(new Friend(10, asList(new Link("www.pqr.com"))))),
                new Person("Xyz", asList(new Friend(12, asList(new Link("www.xyz.com")))))),
            Person.class);
  }

  protected void setup() {
    super.setup();
    fullTableNamePartitioned = testDataset + "." + testTable + "_partitioned";
  }

  @Test
  public void testWriteToBigQueryAppendSaveMode() throws Exception {
    // initial write
    writeToBigQuery(initialData, SaveMode.Append, PARQUET);
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();

    // second write
    writeToBigQuery(additonalData, SaveMode.Append, PARQUET);
    assertThat(testTableNumberOfRows()).isEqualTo(4);
    assertThat(initialDataValuesExist()).isTrue();
  }

  @Test
  void testWriteToBigQueryErrorIfExistsSaveMode() throws Exception {
    // initial write
    writeToBigQuery(initialData, SaveMode.ErrorIfExists, PARQUET);
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
    // second write
    assertThrows(
        IllegalArgumentException.class,
        () -> writeToBigQuery(additonalData, SaveMode.ErrorIfExists, PARQUET));
  }

  @Test
  public void testWriteToBigQueryIgnoreSaveMode() throws Exception {
    // initial write
    writeToBigQuery(initialData, SaveMode.Ignore, PARQUET);
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
    // second write
    writeToBigQuery(additonalData, SaveMode.Ignore, PARQUET);
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
    assertThat(additionalDataValuesExist()).isFalse();
  }

  @Test
  public void testWriteToBigQueryOverwriteSaveMode() throws Exception {
    // initial write
    writeToBigQuery(initialData, SaveMode.Overwrite, PARQUET);
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
    // second write
    writeToBigQuery(additonalData, SaveMode.Overwrite, PARQUET);
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isFalse();
    assertThat(additionalDataValuesExist()).isTrue();
  }

  @Test
  public void testWriteToBigQueryOrcFormat() throws Exception {
    // required by ORC
    spark.conf().set("spark.sql.orc.impl", "native");
    writeToBigQuery(initialData, SaveMode.ErrorIfExists, "orc");
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
  }

  @Test
  public void testWriteToBigQueryAvroFormat() throws Exception {
    writeToBigQuery(initialData, SaveMode.ErrorIfExists, "avro");
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
  }

  @Test
  public void testWriteAllTypesToBigQueryAvroFormat() throws Exception {
       Dataset<Row> allTypesTable = readAllTypesTable();
  writeToBigQuery(allTypesTable, SaveMode.Overwrite, "avro");

    Dataset<Row> df = spark.read().format("bigquery")
          .load(fullTableName);

    assertThat(df.head()).isEqualTo(allTypesTable.head());
    assertThat(df.schema()).isEqualTo(allTypesTable.schema());
}
  @Test
  public void testWriteToBigQueryParquetFormat() throws Exception {
    writeToBigQuery(initialData, SaveMode.ErrorIfExists, "parquet");
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
  }

  @Test
  public void testWriteToBigQueryUnsupportedFormat() throws Exception {
    assertThrows(
        IllegalArgumentException.class,
        () -> writeToBigQuery(initialData, SaveMode.ErrorIfExists, "something else"));
  }

  @Test
  public void testWriteToBigQueryAddingTheSettingsToSparkConf() throws Exception {
    spark.conf().set("temporaryGcsBucket", TEMPORARY_GCS_BUCKET);
    initialData.write().format("bigquery").save(fullTableName);
    assertThat(testTableNumberOfRows()).isEqualTo(2);
    assertThat(initialDataValuesExist()).isTrue();
  }

  @Test
  public void testWriteToBigQueryPartitionedAndClusteredTable() throws Exception {
    Dataset<Row> df =
        spark
            .read()
            .format("com.google.cloud.spark.bigquery")
            .option("table", LIBRARIES_PROJECTS_TABLE)
            .load()
            .where("platform = 'Sublime'");

    df.write()
        .format("bigquery")
        .option("temporaryGcsBucket", TEMPORARY_GCS_BUCKET)
        .option("partitionField", "created_timestamp")
        .option("clusteredFields", "platform")
        .mode(SaveMode.Overwrite)
        .save(fullTableNamePartitioned);

    StandardTableDefinition tableDefinition = getTestPartitionedTableDefinition();
    assertThat(tableDefinition.getTimePartitioning().getField()).isEqualTo("created_timestamp");
    assertThat(tableDefinition.getClustering().getFields()).contains("platform");
  }

//  @Test(timeout = 120) public void testStreamingToBigQueryWriteAppend() throws Exception {
//    StructType schema = initialData.schema();
//    ExpressionEncoder<Row> expressionEncoder = RowEncoder.apply(schema).resolveAndBind();
//    MemoryStream<Row> stream = MemoryStream.apply(expressionEncoder, spark.sqlContext());
//    long lastBatchId = 0L;
//    Dataset<Row> streamingDF = stream.toDF();
//    String cpLoc  = String.format("/tmp/%s-%d",fullTableName, System.nanoTime());
//    // Start write stream
//    StreamingQuery writeStream = streamingDF.writeStream().
//            format("bigquery").
//            outputMode(OutputMode.Append()).
//            option("checkpointLocation", cpLoc).
//            option("table", fullTableName).
//            option("temporaryGcsBucket", TEMPORARY_GCS_BUCKET).
//            start();
//
//    // Write to stream
//  //  stream.addData(initialData.collect());
//    while (writeStream.lastProgress().batchId() <= lastBatchId) {
//      Thread.sleep(1000L);
//    }
//    lastBatchId = writeStream.lastProgress().batchId();
//    assertThat(testTableNumberOfRows()).isEqualTo(2);
//    assertThat(initialDataValuesExist()).isTrue();
//    // Write to stream
//   // stream.addData(additonalData.collect());
//    while (writeStream.lastProgress().batchId() <= lastBatchId) {
//      Thread.sleep(1000L);
//    }
//    writeStream.stop();
//    assertThat(testTableNumberOfRows()).isEqualTo(4);
//    assertThat(additionalDataValuesExist()).isTrue();
//  }

  // helper methods

  int testTableNumberOfRows() {
    // getNumRows returns BigInteger, and it messes up the matchers
    return bq.getTable(testDataset, testTable).getNumRows().intValue();
  }

  StandardTableDefinition getTestPartitionedTableDefinition() {
    return bq.getTable(testDataset, testTable + "_partitioned").getDefinition();
  }

  void writeToBigQuery(Dataset<Row> df, SaveMode mode, String format) {
    df.write()
        .format("bigquery")
        .mode(mode)
        .option("temporaryGcsBucket", TEMPORARY_GCS_BUCKET)
        .option(SparkBigQueryConfig.INTERMEDIATE_FORMAT_OPTION, format)
        .save(fullTableName);
  }

  private long numberOfRowsWith(String name) throws Exception {
    return bq.query(
            QueryJobConfiguration.of(
                String.format("select name from %s where name='%s'", fullTableName, name)))
        .getTotalRows();
  }

  boolean initialDataValuesExist() throws Exception {
    return numberOfRowsWith("Abc") == 1;
  }

  boolean additionalDataValuesExist() throws Exception {
    return numberOfRowsWith("Xyz") == 1;
  }

  static class Person {
    public final String name;
    public final List<Friend> friends;

    public Person(String name, List<Friend> friends) {
      this.name = name;
      this.friends = friends;
    }

    public String getName() {
      return name;
    }

    public List<Friend> getFriends() {
      return friends;
    }
  }

  static class Friend {
    public final int age;
    public final List<Link> links;

    public Friend(int age, List<Link> links) {
      this.age = age;
      this.links = links;
    }

    public int getAge() {
      return age;
    }

    public List<Link> getLinks() {
      return links;
    }
  }

  static class Link {
    public final String uri;

    public Link(String uri) {
      this.uri = uri;
    }

    public String getUri() {
      return uri;
    }
  }
}
