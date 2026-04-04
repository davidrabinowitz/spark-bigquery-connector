package com.google.cloud.bigquery.connector.common;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Dataset;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobInfo.CreateDisposition;
import com.google.cloud.bigquery.JobInfo.WriteDisposition;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.protobuf.CodedOutputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import org.junit.Test;

public class Sandbox {

  @Test
  public void foo() throws Exception {
    BigQuery bq = BigQueryOptions.getDefaultInstance().getService();
    //    Table table =
    //        bq.getTable(
    //            TableId.of("google.com:hadoop-cloud-dev", "tpcds_1T_external", "catalog_sales"));
    //    assertThat(table).isNotNull();

    Schema schema =
        Schema.of(
            Arrays.asList(
                Field.newBuilder("num", LegacySQLTypeName.NUMERIC)
                    .setPrecision(6L)
                    .setScale(1L)
                    .build(),
                Field.newBuilder("bignum", LegacySQLTypeName.BIGNUMERIC)
                    .setPrecision(10L)
                    .setScale(5L)
                    .build()));
    LoadJobConfiguration load =
        LoadJobConfiguration.newBuilder(
                TableId.of("davidrab", "bnload"),
                "gs://davidrab-sandbox/parquet/bn/bn.snappy.parquet",
                FormatOptions.parquet())
            .setSchema(schema)
            .setWriteDisposition(WriteDisposition.WRITE_APPEND)
            .setCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .build();

    Job job = bq.create(JobInfo.of(load)).waitFor();
    System.out.println(job);
  }

  void googleapi() throws Exception {
    Bigquery bigquery = null;
    bigquery.datasets().insert("foo", new Dataset()).execute();
    //    bigquery.
    //    bigquery.jobs().insert("proj", )

  }

  void bar() throws Exception {
    ReadSession re = ReadSession.newBuilder().build();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    re.writeTo(CodedOutputStream.newInstance(baos));
    String base64 = java.util.Base64.getEncoder().encodeToString(baos.toByteArray());
    System.out.println(base64);
  }

  @Test
  public void wordle() throws Exception {
    char[] a = "וטכגדצסז".toCharArray();
    for (char c1 : a) {
      //      if (c1 ==  'ה' || c1 ==  'מ') {
      //        continue;
      //      }
      for (char c2 : a) {
        if (c2 != 'ו') {
          continue;
        }
        for (char c3 : a) {
          if (c3 == 'ו' || c3 == 'ד') {
            continue;
          }
          for (char c4 : a) {
            if (c4 == 'ו' || c4 == 'כ') {
              continue;
            }
            for (char c5 : a) {
              if (c5 == 'מ') {
                continue;
              }
              String word = "" + c1 + "" + c2 + "" + c3 + "" + c4 + "" + c5;
              if (word.indexOf('ד') == -1 || word.indexOf('כ') == -1) {
                continue;
              }
              boolean ok = true;
              //              for (char c : a) {
              //                if (word.indexOf(c) == -1) {
              //                  ok = false;
              //                  break;
              //                }
              //              }
              if (ok) {
                System.out.println(word);
              }
            }
          }
        }
      }
    }
  }
}
