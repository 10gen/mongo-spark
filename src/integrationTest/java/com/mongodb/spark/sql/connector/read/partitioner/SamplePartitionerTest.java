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

package com.mongodb.spark.sql.connector.read.partitioner;

import static com.mongodb.spark.sql.connector.config.ReadConfig.PARTITIONER_OPTIONS_PREFIX;
import static com.mongodb.spark.sql.connector.read.partitioner.FieldListPartitioner.PARTITION_FIELD_LIST_CONFIG;
import static com.mongodb.spark.sql.connector.read.partitioner.PartitionerHelper.SINGLE_PARTITIONER;
import static com.mongodb.spark.sql.connector.read.partitioner.PartitionerHelper.createPartitionPipeline;
import static com.mongodb.spark.sql.connector.read.partitioner.SamplePartitioner.PARTITION_SIZE_MB_CONFIG;
import static com.mongodb.spark.sql.connector.read.partitioner.SamplePartitioner.SAMPLES_PER_PARTITION_CONFIG;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

import org.bson.BsonDocument;

import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.exceptions.ConfigException;
import com.mongodb.spark.sql.connector.read.MongoInputPartition;

public class SamplePartitionerTest extends PartitionerTestCase {

  private static final Partitioner PARTITIONER = new SamplePartitioner();

  @Override
  List<String> defaultReadConfigOptions() {
    return asList(
        ReadConfig.PARTITIONER_OPTIONS_PREFIX + PARTITION_SIZE_MB_CONFIG,
        "1",
        ReadConfig.PARTITIONER_OPTIONS_PREFIX + SamplePartitioner.SAMPLES_PER_PARTITION_CONFIG,
        "10");
  }

  @Test
  void testNonExistentCollection() {
    List<MongoInputPartition> partitions = PARTITIONER.generatePartitions(createReadConfig());
    assertIterableEquals(SINGLE_PARTITIONER.generatePartitions(createReadConfig()), partitions);
  }

  @Test
  void testFewerRecordsThanData() {
    ReadConfig readConfig =
        createReadConfig(ReadConfig.PARTITIONER_OPTIONS_PREFIX + PARTITION_SIZE_MB_CONFIG, "4");
    loadSampleData(10, 2, readConfig);

    List<MongoInputPartition> partitions = PARTITIONER.generatePartitions(readConfig);
    assertIterableEquals(SINGLE_PARTITIONER.generatePartitions(readConfig), partitions);

    readConfig =
        createReadConfig(ReadConfig.PARTITIONER_OPTIONS_PREFIX + PARTITION_SIZE_MB_CONFIG, "2");
    partitions = PARTITIONER.generatePartitions(readConfig);
    assertIterableEquals(SINGLE_PARTITIONER.generatePartitions(readConfig), partitions);
  }

  @Test
  void testCreatesExpectedPartitions() {
    ReadConfig readConfig = createReadConfig();
    loadSampleData(51, 5, readConfig);

    List<MongoInputPartition> expectedPartitions =
        asList(
            new MongoInputPartition(
                0,
                createPartitionPipeline(BsonDocument.parse("{_id: {$lt: '00010'}}"), emptyList()),
                getPreferredLocations()),
            new MongoInputPartition(
                1,
                createPartitionPipeline(
                    BsonDocument.parse("{_id: {$gte: '00010', $lt: '00020'}}"), emptyList()),
                getPreferredLocations()),
            new MongoInputPartition(
                2,
                createPartitionPipeline(
                    BsonDocument.parse("{_id: {$gte: '00020', $lt: '00030'}}"), emptyList()),
                getPreferredLocations()),
            new MongoInputPartition(
                3,
                createPartitionPipeline(
                    BsonDocument.parse("{_id: {$gte: '00030', $lt: '00040'}}"), emptyList()),
                getPreferredLocations()),
            new MongoInputPartition(
                4,
                createPartitionPipeline(
                    BsonDocument.parse("{_id: {$gte: '00040', $lt: '00050'}}"), emptyList()),
                getPreferredLocations()),
            new MongoInputPartition(
                5,
                createPartitionPipeline(BsonDocument.parse("{_id: {$gte: '00050'}}"), emptyList()),
                getPreferredLocations()));

    assertIterableEquals(expectedPartitions, PARTITIONER.generatePartitions(readConfig));
  }

  @Test
  void testUsingAlternativePartitionFieldList() {
    ReadConfig readConfig =
        createReadConfig(
            ReadConfig.PARTITIONER_OPTIONS_PREFIX + SamplePartitioner.PARTITION_FIELD_LIST_CONFIG,
            "pk");
    loadSampleData(51, 5, readConfig);

    List<MongoInputPartition> expectedPartitions =
        asList(
            new MongoInputPartition(
                0,
                createPartitionPipeline(BsonDocument.parse("{pk: {$lt: '_10010'}}"), emptyList()),
                getPreferredLocations()),
            new MongoInputPartition(
                1,
                createPartitionPipeline(
                    BsonDocument.parse("{pk: {$gte: '_10010', $lt: '_10020'}}"), emptyList()),
                getPreferredLocations()),
            new MongoInputPartition(
                2,
                createPartitionPipeline(
                    BsonDocument.parse("{pk: {$gte: '_10020', $lt: '_10030'}}"), emptyList()),
                getPreferredLocations()),
            new MongoInputPartition(
                3,
                createPartitionPipeline(
                    BsonDocument.parse("{pk: {$gte: '_10030', $lt: '_10040'}}"), emptyList()),
                getPreferredLocations()),
            new MongoInputPartition(
                4,
                createPartitionPipeline(
                    BsonDocument.parse("{pk: {$gte: '_10040', $lt: '_10050'}}"), emptyList()),
                getPreferredLocations()),
            new MongoInputPartition(
                5,
                createPartitionPipeline(BsonDocument.parse("{pk: {$gte: '_10050'}}"), emptyList()),
                getPreferredLocations()));

    assertIterableEquals(expectedPartitions, PARTITIONER.generatePartitions(readConfig));
  }

  @Test
  void testUsingPartitionFieldThatContainsDuplicates() {
    ReadConfig readConfig =
        createReadConfig(PARTITIONER_OPTIONS_PREFIX + PARTITION_FIELD_LIST_CONFIG, "dups");
    ReadConfig multiFieldOneContainsInvalidBounds =
        createReadConfig(PARTITIONER_OPTIONS_PREFIX + PARTITION_FIELD_LIST_CONFIG, "_id, dups");

    loadSampleData(101, 20, readConfig);

    assertThrows(ConfigException.class, () -> PARTITIONER.generatePartitions(readConfig));
    assertThrows(
        ConfigException.class,
        () -> PARTITIONER.generatePartitions(multiFieldOneContainsInvalidBounds));
  }

  @Test
  void testUsingMultipleFieldsInThePartitionFieldList() {
    ReadConfig readConfig =
        createReadConfig(
            ReadConfig.PARTITIONER_OPTIONS_PREFIX + SamplePartitioner.PARTITION_FIELD_LIST_CONFIG,
            "_id, pk");
    loadSampleData(51, 5, readConfig);

    List<MongoInputPartition> expectedPartitions =
        asList(
            new MongoInputPartition(
                0,
                createPartitionPipeline(
                    BsonDocument.parse("{_id: {$lt: '00010'}, pk: {$lt: '_10010'}}"), emptyList()),
                getPreferredLocations()),
            new MongoInputPartition(
                1,
                createPartitionPipeline(
                    BsonDocument.parse(
                        "{_id: {$gte: '00010', $lt: '00020'}, pk: {$gte: '_10010', $lt: '_10020'}}"),
                    emptyList()),
                getPreferredLocations()),
            new MongoInputPartition(
                2,
                createPartitionPipeline(
                    BsonDocument.parse(
                        "{_id: {$gte: '00020', $lt: '00030'}, pk: {$gte: '_10020', $lt: '_10030'}}"),
                    emptyList()),
                getPreferredLocations()),
            new MongoInputPartition(
                3,
                createPartitionPipeline(
                    BsonDocument.parse(
                        "{_id: {$gte: '00030', $lt: '00040'}, pk: {$gte: '_10030', $lt: '_10040'}}"),
                    emptyList()),
                getPreferredLocations()),
            new MongoInputPartition(
                4,
                createPartitionPipeline(
                    BsonDocument.parse(
                        "{_id: {$gte: '00040', $lt: '00050'}, pk: {$gte: '_10040', $lt: '_10050'}}"),
                    emptyList()),
                getPreferredLocations()),
            new MongoInputPartition(
                5,
                createPartitionPipeline(
                    BsonDocument.parse("{_id: {$gte: '00050'}, pk: {$gte: '_10050'}}"),
                    emptyList()),
                getPreferredLocations()));

    assertIterableEquals(expectedPartitions, PARTITIONER.generatePartitions(readConfig));
  }

  @Test
  void testCreatesExpectedPartitionsWithUsersPipeline() {
    ReadConfig readConfig =
        createReadConfig(
            ReadConfig.AGGREGATION_PIPELINE_CONFIG,
            "{'$match': {'_id': {'$gte': '00010', '$lte': '00040'}}}");
    List<BsonDocument> userSuppliedPipeline =
        singletonList(
            BsonDocument.parse("{'$match': {'_id': {'$gte': '00010', " + "'$lte': '00040'}}}"));

    // No data
    assertIterableEquals(
        SINGLE_PARTITIONER.generatePartitions(readConfig),
        PARTITIONER.generatePartitions(readConfig));

    loadSampleData(50, 10, readConfig);
    List<MongoInputPartition> expectedPartitions =
        asList(
            new MongoInputPartition(
                0,
                createPartitionPipeline(
                    BsonDocument.parse("{_id: {$lt: '00020'}}"), userSuppliedPipeline),
                getPreferredLocations()),
            new MongoInputPartition(
                1,
                createPartitionPipeline(
                    BsonDocument.parse("{_id: {$gte: '00020', $lt: '00030'}}"),
                    userSuppliedPipeline),
                getPreferredLocations()),
            new MongoInputPartition(
                2,
                createPartitionPipeline(
                    BsonDocument.parse("{_id: {$gte: '00030', $lt: '00040'}}"),
                    userSuppliedPipeline),
                getPreferredLocations()),
            new MongoInputPartition(
                3,
                createPartitionPipeline(
                    BsonDocument.parse("{_id: {$gte: '00040'}}"), userSuppliedPipeline),
                getPreferredLocations()));

    assertIterableEquals(expectedPartitions, PARTITIONER.generatePartitions(readConfig));
  }

  @Test
  void shouldValidateReadConfigs() {
    loadSampleData(50, 2, createReadConfig());

    assertAll(
        () ->
            assertThrows(
                ConfigException.class,
                () ->
                    PARTITIONER.generatePartitions(
                        createReadConfig(
                            PARTITIONER_OPTIONS_PREFIX + SAMPLES_PER_PARTITION_CONFIG, "-1")),
                SAMPLES_PER_PARTITION_CONFIG + " is negative"),
        () ->
            assertThrows(
                ConfigException.class,
                () ->
                    PARTITIONER.generatePartitions(
                        createReadConfig(
                            PARTITIONER_OPTIONS_PREFIX + SAMPLES_PER_PARTITION_CONFIG, "0")),
                SAMPLES_PER_PARTITION_CONFIG + " is zero"),
        () ->
            assertThrows(
                ConfigException.class,
                () ->
                    PARTITIONER.generatePartitions(
                        createReadConfig(
                            PARTITIONER_OPTIONS_PREFIX + SAMPLES_PER_PARTITION_CONFIG, "1")),
                SAMPLES_PER_PARTITION_CONFIG + " is one"),
        () ->
            assertThrows(
                ConfigException.class,
                () ->
                    PARTITIONER.generatePartitions(
                        createReadConfig(
                            PARTITIONER_OPTIONS_PREFIX + PARTITION_SIZE_MB_CONFIG, "-1")),
                PARTITION_SIZE_MB_CONFIG + " is negative"),
        () ->
            assertThrows(
                ConfigException.class,
                () ->
                    PARTITIONER.generatePartitions(
                        createReadConfig(
                            PARTITIONER_OPTIONS_PREFIX + PARTITION_SIZE_MB_CONFIG, "0")),
                PARTITION_SIZE_MB_CONFIG + " is zero"));
  }
}
