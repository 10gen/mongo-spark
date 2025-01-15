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

import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/**
 * An internal SparkHadoopUtils helper for creating Hadoop configuration
 */
public final class SparkHadoopUtils {

  private static final String FILESYSTEM_CONFIGURATION_PREFIX = "fs.";

  /**
   * A helper that creates a Hadoop Configuration that includes all {@code fs.} prefixed configurations.
   * <p>
   * For cluster wide hadoop configurations users use the {@code spark.hadoop} prefix for configurations.
   * However, existing azure documentation doesn't include the {@code spark.hadoop} prefix and just uses {@code fs.azure.}.
   * This helper sets any filesystem configuration prefixed with {@code fs.} on top of the created
   * {@code sparkContext.hadoopConfiguration()} configuration.
   *
   * @see <a href="https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage">Azure-storage docs</a>
   * @return the Hadoop Configuration
   */
  public static Configuration createHadoopConfiguration() {
    SparkContext sparkContext = SparkContext.getOrCreate();
    Configuration hadoopConfiguration = sparkContext.hadoopConfiguration();

    SparkConf sparkConf = sparkContext.getConf();
    Arrays.stream(sparkConf.getAll())
        .filter(kv -> kv._1.startsWith(FILESYSTEM_CONFIGURATION_PREFIX))
        .forEach(kv -> hadoopConfiguration.set(kv._1, kv._2));

    return hadoopConfiguration;
  }

  private SparkHadoopUtils() {}
}
