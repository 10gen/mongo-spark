/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.mongodb.spark.sql.connector.utils;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorTestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Test;

class SparkHadoopUtilsTest extends MongoSparkConnectorTestCase {
  private static final String SPARK_HADOOP_FS_AZURE_ACCOUNT_AUTH_TYPE =
      "spark.hadoop.fs.azure.account.auth.type";
  private static final String FS_AZURE_ACCOUNT_AUTH_TYPE = "fs.azure.account.auth.type";
  private static final String FS_AZURE_ACCOUNT_AUTH_TYPE_VALUE = "OAUTH";

  @Test
  void testDefaultHadoopConfiguration() {
    SparkConf sparkConf = getSparkConf();
    sparkConf.set(FS_AZURE_ACCOUNT_AUTH_TYPE, FS_AZURE_ACCOUNT_AUTH_TYPE_VALUE);

    Configuration hadoopConfiguration = createSparkContext(sparkConf).hadoopConfiguration();
    assertConfigValue(hadoopConfiguration, FS_AZURE_ACCOUNT_AUTH_TYPE, null);
  }

  @Test
  void testSparkHadoopPrefix() {
    SparkConf sparkConf = getSparkConf();
    sparkConf.set(SPARK_HADOOP_FS_AZURE_ACCOUNT_AUTH_TYPE, FS_AZURE_ACCOUNT_AUTH_TYPE_VALUE);

    Configuration hadoopConfiguration = createSparkContext(sparkConf).hadoopConfiguration();

    assertConfigValue(
        hadoopConfiguration, FS_AZURE_ACCOUNT_AUTH_TYPE, FS_AZURE_ACCOUNT_AUTH_TYPE_VALUE);
  }

  @Test
  void testFSPrefix() {
    SparkConf sparkConf = getSparkConf();
    sparkConf.set(FS_AZURE_ACCOUNT_AUTH_TYPE, FS_AZURE_ACCOUNT_AUTH_TYPE_VALUE);
    createSparkContext(sparkConf);

    Configuration hadoopConfiguration = SparkHadoopUtils.createHadoopConfiguration();

    assertConfigValue(
        hadoopConfiguration, FS_AZURE_ACCOUNT_AUTH_TYPE, FS_AZURE_ACCOUNT_AUTH_TYPE_VALUE);
  }

  @Test
  void testFSPrefixTakesPrecedence() {
    SparkConf sparkConf = getSparkConf();
    sparkConf.set(SPARK_HADOOP_FS_AZURE_ACCOUNT_AUTH_TYPE, "DEFAULT VALUE");
    sparkConf.set(FS_AZURE_ACCOUNT_AUTH_TYPE, FS_AZURE_ACCOUNT_AUTH_TYPE_VALUE);
    createSparkContext(sparkConf);

    Configuration hadoopConfiguration = SparkHadoopUtils.createHadoopConfiguration();

    assertConfigValue(
        hadoopConfiguration, FS_AZURE_ACCOUNT_AUTH_TYPE, FS_AZURE_ACCOUNT_AUTH_TYPE_VALUE);
  }

  /**
   * Assert that a hadoop configuration option has the expected value.
   * @param hadoopConf configuration to query
   * @param key key to look up
   * @param expected expected value.
   */
  private void assertConfigValue(
      final Configuration hadoopConf, final String key, final String expected) {
    assertEquals(expected, hadoopConf.get(key), format("Mismatch in expected value of %s", key));
  }
}
