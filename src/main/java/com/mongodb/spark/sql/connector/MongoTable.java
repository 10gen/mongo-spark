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

package com.mongodb.spark.sql.connector;

import static java.util.Arrays.asList;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.write.MongoWriteBuilder;

/**
 * Represents a MongoDB Collection.
 *
 * <p>Implements {@link SupportsWrite} for writing to a MongoDB collection
 */
public class MongoTable implements Table, SupportsWrite {

  private static final Set<TableCapability> TABLE_CAPABILITY_SET =
      new HashSet<>(
          asList(
              TableCapability.BATCH_WRITE,
              TableCapability.TRUNCATE,
              TableCapability.STREAMING_WRITE,
              TableCapability.ACCEPT_ANY_SCHEMA));
  private final StructType schema;
  private final Transform[] partitioning;
  private final MongoConfig mongoConfig;

  /**
   * Construct a new instance
   *
   * @param mongoConfig The specified table configuration
   */
  public MongoTable(final MongoConfig mongoConfig) {
    this(new StructType(), mongoConfig);
  }

  /**
   * Construct a new instance
   *
   * @param schema The specified table schema.
   * @param mongoConfig The specified table configuration
   */
  public MongoTable(final StructType schema, final MongoConfig mongoConfig) {
    this(schema, new Transform[0], mongoConfig);
  }

  /**
   * Construct a new instance
   *
   * @param schema The specified table schema.
   * @param partitioning The specified table partitioning.
   * @param mongoConfig The specified table configuration
   */
  public MongoTable(
      final StructType schema, final Transform[] partitioning, final MongoConfig mongoConfig) {
    this.schema = schema;
    this.partitioning = partitioning;
    this.mongoConfig = mongoConfig;
  }

  /**
   * Returns a {@link WriteBuilder} that spark will call and configure each data source write.
   *
   * @param info the logical write info
   */
  @Override
  public WriteBuilder newWriteBuilder(final LogicalWriteInfo info) {
    return new MongoWriteBuilder(info, mongoConfig.toWriteConfig());
  }

  /**
   * A name to identify this table. Implementations should provide a meaningful name, like the
   * database and table name from catalog, or the location of files for this table.
   */
  @Override
  public String name() {
    return "MongoTable(" + mongoConfig.getNamespace() + ")";
  }

  /**
   * Returns the schema of this table. If the table is not readable and doesn't have a schema, an
   * empty schema can be returned here.
   */
  @Override
  public StructType schema() {
    Assertions.ensureState(() -> Objects.nonNull(schema), "Schema cannot be null");
    return schema;
  }

  @Override
  public Transform[] partitioning() {
    return partitioning;
  }

  @Override
  public Map<String, String> properties() {
    return mongoConfig.getOriginals();
  }

  /** Returns the set of capabilities for this table. */
  @Override
  public Set<TableCapability> capabilities() {
    return TABLE_CAPABILITY_SET;
  }

  @Override
  public String toString() {
    return "MongoTable{"
        + "schema="
        + schema
        + ", partitioning="
        + Arrays.toString(partitioning)
        + ", mongoConfig="
        + mongoConfig
        + '}';
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final MongoTable that = (MongoTable) o;
    return Objects.equals(schema, that.schema)
        && Arrays.equals(partitioning, that.partitioning)
        && Objects.equals(mongoConfig, that.mongoConfig);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(schema, mongoConfig);
    result = 31 * result + Arrays.hashCode(partitioning);
    return result;
  }
}
