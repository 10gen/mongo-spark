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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.spark.connector.Versions;
import com.mongodb.spark.sql.connector.ExpressionConverter.FilterAndPipelineStage;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.config.WriteConfig;
import com.mongodb.spark.sql.connector.read.MongoScanBuilder;
import com.mongodb.spark.sql.connector.write.MongoWriteBuilder;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.connector.catalog.SupportsDelete;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Represents a MongoDB Collection. */
final class MongoTable implements Table, SupportsWrite, SupportsRead, SupportsDelete {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoTable.class);
  private static final Set<TableCapability> TABLE_CAPABILITY_SET = new HashSet<>(asList(
      TableCapability.BATCH_WRITE,
      TableCapability.TRUNCATE,
      TableCapability.STREAMING_WRITE,
      TableCapability.ACCEPT_ANY_SCHEMA,
      TableCapability.BATCH_READ,
      TableCapability.MICRO_BATCH_READ,
      TableCapability.CONTINUOUS_READ));
  private final StructType schema;
  private final Transform[] partitioning;
  private final MongoConfig mongoConfig;

  /**
   * Construct a new instance
   *
   * @param mongoConfig The specified table configuration
   */
  MongoTable(final MongoConfig mongoConfig) {
    this(new StructType(), mongoConfig);
  }

  /**
   * Construct a new instance
   *
   * @param schema The specified table schema.
   * @param mongoConfig The specified table configuration
   */
  MongoTable(final StructType schema, final MongoConfig mongoConfig) {
    this(schema, new Transform[0], mongoConfig);
  }

  /**
   * Construct a new instance
   *
   * @param schema The specified table schema.
   * @param partitioning The specified table partitioning.
   * @param mongoConfig The specified table configuration
   */
  MongoTable(
      final StructType schema, final Transform[] partitioning, final MongoConfig mongoConfig) {
    LOGGER.info("Creating MongoTable: {}-{}", Versions.NAME, Versions.VERSION);
    this.schema = schema;
    this.partitioning = partitioning;
    this.mongoConfig = mongoConfig;
  }

  /** The name of this table. */
  @Override
  public String name() {
    if (mongoConfig instanceof ReadConfig) {
      return "MongoTable(" + mongoConfig.toReadConfig().getNamespaceDescription() + ")";
    } else if (mongoConfig instanceof WriteConfig) {
      return "MongoTable(" + mongoConfig.toWriteConfig().getNamespaceDescription() + ")";
    } else {
      return "MongoTable()";
    }
  }

  /**
   * Returns a {@link MongoWriteBuilder}.
   *
   * @param info the logical write info
   */
  @Override
  public WriteBuilder newWriteBuilder(final LogicalWriteInfo info) {
    return new MongoWriteBuilder(info, mongoConfig.toWriteConfig());
  }

  /**
   * Returns a {@link MongoScanBuilder}.
   *
   * @param options the {@link CaseInsensitiveStringMap} with configuration regarding the read
   */
  @Override
  public ScanBuilder newScanBuilder(final CaseInsensitiveStringMap options) {
    return new MongoScanBuilder(
        schema, mongoConfig.toReadConfig().withOptions(options.asCaseSensitiveMap()));
  }

  /** Returns the schema of this table. */
  @Override
  public StructType schema() {
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

  @Override
  public void deleteWhere(final Filter[] filters) {
      ExpressionConverter converter = new ExpressionConverter(schema);

      List<Bson> stages = Arrays.stream(filters)
              .map(converter::processFilter)
              .filter(FilterAndPipelineStage::hasPipelineStage)
              .map(FilterAndPipelineStage::getPipelineStage)
              .collect(Collectors.toList());
      Bson query = Filters.and(stages);
      WriteConfig writeConfig = mongoConfig.toWriteConfig();

      MongoClient mongoClient = writeConfig.getMongoClient();
      MongoDatabase database = mongoClient.getDatabase(writeConfig.getDatabaseName());
      MongoCollection<Document> collection = database.getCollection(writeConfig.getCollectionName());
      collection.deleteMany(query);
  }
}
