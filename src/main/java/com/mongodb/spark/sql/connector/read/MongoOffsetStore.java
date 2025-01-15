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
package com.mongodb.spark.sql.connector.read;

import static java.lang.String.format;

import com.mongodb.spark.sql.connector.exceptions.ConfigException;
import com.mongodb.spark.sql.connector.utils.SparkHadoopUtils;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Mongo offset store for streams. */
final class MongoOffsetStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoOffsetStore.class);
  private final Path checkpointLocation;
  private final FileSystem fs;
  private MongoOffset offset;

  /**
   * Instantiates a new Mongo offset store.
   *
   * @param checkpointLocation the checkpoint location for offsets
   * @param offset the offset
   */
  MongoOffsetStore(final String checkpointLocation, final MongoOffset offset) {
    try {
      this.fs = FileSystem.get(
          URI.create(checkpointLocation), SparkHadoopUtils.createHadoopConfiguration());
    } catch (IOException e) {
      throw new ConfigException(
          format("Unable to initialize the MongoOffsetStore: %s", checkpointLocation), e);
    }
    this.checkpointLocation = new Path(URI.create(checkpointLocation));
    this.offset = offset;
  }

  /**
   * Initial offset Mongo offset.
   *
   * @return the Mongo offset
   */
  public MongoOffset initialOffset() {
    boolean exists;
    try {
      exists = fs.exists(checkpointLocation);
    } catch (IOException e) {
      throw new ConfigException(
          format("Unable to determine if the checkpoint location exists: %s", checkpointLocation),
          e);
    }

    if (exists) {
      try (FSDataInputStream in = fs.open(checkpointLocation)) {
        byte[] buf = IOUtils.toByteArray(in);
        offset = MongoOffset.fromJson(new String(buf, StandardCharsets.UTF_8));
      } catch (IOException exception) {
        throw new ConfigException(
            format("Failed to parse offset from: %s", checkpointLocation), exception);
      }
    } else {
      updateOffset(offset);
    }
    LOGGER.info("Initial offset: {}", offset);
    return offset;
  }

  /**
   * Update offset.
   *
   * @param offset the offset
   */
  public void updateOffset(final MongoOffset offset) {
    try (FSDataOutputStream out = fs.create(checkpointLocation, true)) {
      out.write(offset.json().getBytes(StandardCharsets.UTF_8));
      out.hflush();
    } catch (IOException exception) {
      throw new ConfigException(
          format("Failed to update new offset to: %s at %s", offset, checkpointLocation),
          exception);
    }
  }
}
