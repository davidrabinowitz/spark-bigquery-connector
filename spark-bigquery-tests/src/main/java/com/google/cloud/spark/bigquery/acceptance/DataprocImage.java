/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.acceptance;

public enum DataprocImage {
  V1_3("1.3-debian10", "2.11"),
  V1_4("1.4-debian10", "2.11"),
  V1_5("1.5-debian10", "2.12"),
  V2_0("2.0-debian10", "2.12");

  final private String imageVersion;
  final private String scalaVersion;

  DataprocImage(String imageVersion, String scalaVersion) {
    this.imageVersion = imageVersion;
    this.scalaVersion = scalaVersion;
  }

  public String getImageVersion() {
    return imageVersion;
  }

  public String getScalaVersion() {
    return scalaVersion;
  }
}
