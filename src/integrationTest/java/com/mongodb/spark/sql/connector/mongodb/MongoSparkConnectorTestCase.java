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
 */
package com.mongodb.spark.sql.connector.mongodb;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.config.WriteConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.opentest4j.AssertionFailedError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@MongoDBOnline()
public class MongoSparkConnectorTestCase {

  public static final Logger LOGGER = LoggerFactory.getLogger(MongoSparkConnectorTestCase.class);

  @RegisterExtension
  public static final MongoSparkConnectorHelper HELPER = new MongoSparkConnectorHelper();

  public static final String IGNORE_COMMENT = "IGNORE_THIS_COMMENT";

  public static final String TEST_COMMENT = "TEST_COMMENT";

  public String getDatabaseName() {
    return HELPER.getDatabaseName();
  }

  public String getCollectionName() {
    return HELPER.getCollectionName();
  }

  public MongoDatabase getDatabase() {
    return HELPER.getDatabase();
  }

  public MongoCollection<BsonDocument> getCollection() {
    return getCollection(getCollectionName());
  }

  public MongoCollection<BsonDocument> getCollection(final String collectionName) {
    return getDatabase().getCollection(collectionName, BsonDocument.class);
  }

  public boolean supportsChangeStreams() {
    MongoClient mongoClient = HELPER.getMongoClient();
    ClusterType clusterType = mongoClient.getClusterDescription().getType();
    int counter = 0;
    while (clusterType == ClusterType.UNKNOWN && counter < 30) {
      HELPER.sleep(1000, "Interrupted when checking change stream support");
      clusterType = mongoClient.getClusterDescription().getType();
      counter++;
    }
    return clusterType == ClusterType.SHARDED || clusterType == ClusterType.REPLICA_SET;
  }

  public boolean isAtLeastFourDotFour() {
    return getMaxWireVersion() >= 9;
  }

  public boolean isAtLeastFiveDotZero() {
    return getMaxWireVersion() >= 12;
  }

  private int getMaxWireVersion() {
    MongoClient mongoClient = HELPER.getMongoClient();
    ClusterType clusterType = mongoClient.getClusterDescription().getType();
    int counter = 0;
    while (clusterType == ClusterType.UNKNOWN && counter < 30) {
      HELPER.sleep(1000, "Interrupted when max wire version");
      clusterType = mongoClient.getClusterDescription().getType();
      counter++;
    }

    return HELPER.getMongoClient().getClusterDescription().getServerDescriptions().stream()
        .map(ServerDescription::getMaxWireVersion)
        .max(Integer::compare)
        .orElse(0);
  }

  public boolean isSharded() {
    return HELPER.isSharded();
  }

  public void shardCollection(final MongoNamespace mongoNamespace, final String shardKeyJson) {
    HELPER.shardCollection(mongoNamespace, shardKeyJson);
  }

  public CaseInsensitiveStringMap getConnectionProviderOptions() {
    Map<String, String> options = new HashMap<>();
    options.put(
        MongoConfig.PREFIX + MongoConfig.CONNECTION_STRING_CONFIG,
        HELPER.getConnectionString().toString());
    return new CaseInsensitiveStringMap(options);
  }

  public MongoConfig getMongoConfig() {
    Map<String, String> options = new HashMap<>();
    options.put(
        MongoConfig.PREFIX + MongoConfig.CONNECTION_STRING_CONFIG,
        HELPER.getConnectionString().toString());
    options.put(MongoConfig.PREFIX + MongoConfig.DATABASE_NAME_CONFIG, getDatabaseName());
    options.put(MongoConfig.PREFIX + MongoConfig.COLLECTION_NAME_CONFIG, getCollectionName());
    return MongoConfig.createConfig(options);
  }

  /**
   * Creates sample data
   *
   * @param numberOfDocuments the total number of documents to create
   * @param sizeInMB the total size of the documents
   * @param config the config used for the database and collection
   */
  public void loadSampleData(
      final int numberOfDocuments, final int sizeInMB, final MongoConfig config) {
    HELPER.loadSampleData(numberOfDocuments, sizeInMB, config);
  }

  /**
   * Creates complex sample data
   *
   * @param numberOfDocuments the total number of documents to create
   * @param sizeInMB the total size of the documents
   * @param config the config used for the database and collection
   */
  public void loadComplexSampleData(
      final int numberOfDocuments, final int sizeInMB, final MongoConfig config) {
    HELPER.loadComplexSampleData(numberOfDocuments, sizeInMB, config);
  }

  /** Runs events with the profiler on. */
  public void assertCommentsInProfile(final Runnable runnable, final ReadConfig readConfig) {
    assumeFalse(isSharded());
    assertNotNull(readConfig.getComment());
    assertCommentsInProfile(runnable, readConfig.getDatabaseName(), readConfig.getCollectionName());
  }

  /** Runs events with the profiler on. */
  public void assertCommentsInProfile(final Runnable runnable, final WriteConfig writeConfig) {
    assertNotNull(writeConfig.getComment());
    assertCommentsInProfile(
        runnable, writeConfig.getDatabaseName(), writeConfig.getCollectionName());
  }

  private void assertCommentsInProfile(
      final Runnable runnable, final String databaseName, final String collectionName) {
    assumeTrue(isAtLeastFiveDotZero());
    assumeFalse(isSharded());
    MongoDatabase database = HELPER.getMongoClient().getDatabase(databaseName);
    MongoCollection<Document> profileCollection = database.getCollection("system.profile");
    try {
      profileCollection.drop();
      database.runCommand(new Document("profile", 1)
          .append(
              "filter",
              Filters.eq("ns", new MongoNamespace(databaseName, collectionName).getFullName())));

      runnable.run();

      List<Document> profileDocsWithComment = profileCollection
          .find(Filters.eq("command.comment", TEST_COMMENT))
          .into(new ArrayList<>());
      assertFalse(
          profileDocsWithComment.isEmpty(),
          format("There were no commands observed with the comment \"%s\"", TEST_COMMENT));

      List<Document> profileDocs = profileCollection
          .find(Filters.nor(
              Filters.exists("command.killCursors"), Filters.eq("command.comment", IGNORE_COMMENT)))
          .into(new ArrayList<>());

      List<String> withoutComment = profileDocs.stream()
          .filter(d -> !d.getEmbedded(asList("command", "comment"), "").equals(TEST_COMMENT))
          .map(Document::toJson)
          .collect(Collectors.toList());

      assertTrue(
          withoutComment.isEmpty(),
          () ->
              format("The following commands were observed without comments: %s", withoutComment));
    } finally {
      database.runCommand(BsonDocument.parse("{profile: 0}"));
      profileCollection.drop();
    }
  }

  public SparkConf getSparkConf() {
    return HELPER.getSparkConf();
  }

  public SparkSession getOrCreateSparkSession() {
    return getOrCreateSparkSession(getSparkConf());
  }

  public SparkSession getOrCreateSparkSession(final SparkConf sparkConfig) {
    return SparkSession.builder()
        .sparkContext(getOrCreateSparkContext(sparkConfig))
        .getOrCreate();
  }

  public SparkContext getOrCreateSparkContext(final SparkConf sparkConfig) {
    return HELPER.getOrCreateSparkContext(sparkConfig);
  }

  public SparkContext createSparkContext(final SparkConf sparkConfig) {
    return HELPER.getOrCreateSparkContext(sparkConfig, true);
  }

  public void retryAssertion(final Runnable assertion) {
    retryAssertion(assertion, () -> {});
  }

  public void retryAssertion(final Runnable assertion, final Runnable onFailure) {
    int retries = 5;
    int counter = 0;
    int timeoutMs = 2000;
    boolean hasError = true;
    AssertionFailedError exception = null;
    while (counter < retries && hasError) {
      try {
        counter++;
        LOGGER.info("Assertion attempt: {}", counter);
        assertion.run();
        hasError = false;
      } catch (AssertionFailedError e) {
        LOGGER.info(
            "Failed assertion attempt: {} timeout: {}ms. {}", counter, timeoutMs, e.getMessage());
        exception = e;
        onFailure.run();
        if (counter < retries) {
          LOGGER.info("SLEEPING {}ms", timeoutMs);
          HELPER.sleep(timeoutMs, "Interrupted when retrying assertion.");
          timeoutMs += timeoutMs;
        }
      }
    }
    if (hasError) {
      throw exception;
    }
  }
}
