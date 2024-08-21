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
import static org.junit.jupiter.api.Assertions.fail;

import com.mongodb.ConnectionString;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.connection.ClusterType;
import com.mongodb.spark.sql.connector.MongoCatalog;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoSparkConnectorHelper
    implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback {
  private static final String DEFAULT_URI = "mongodb://localhost:27017";
  public static final String URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";
  public static final String DEFAULT_DATABASE_NAME = "MongoSparkConnectorTest";
  public static final String DEFAULT_COLLECTION_NAME = "coll";
  private static final String SAMPLE_DATA_TEMPLATE =
      "{_id: '%s', pk: '%s', dups: '%s', i: %d, s: '%s'}";
  private static final String COMPLEX_SAMPLE_DATA_TEMPLATE =
      "{_id: '%s', nested: {pk: '%s', dups: '%s', i: %d}, s: '%s'}";
  public static final String CATALOG = "mongo_catalog";

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoSparkConnectorHelper.class);

  private SparkContext sparkContext;
  private ConnectionString connectionString;
  private MongoClient mongoClient;
  private Boolean online;
  private File tmpDirectory;

  public MongoSparkConnectorHelper() {}

  public MongoClient getMongoClient() {
    if (mongoClient == null) {
      mongoClient = MongoClients.create(getConnectionString());
    }
    return mongoClient;
  }

  @Override
  public void beforeAll(final ExtensionContext context) {
    getOrCreateSparkContext(getSparkConf());
  }

  @Override
  public void beforeEach(final ExtensionContext context) {
    if (mongoClient != null) {
      getDatabase().drop();
    }
    if (tmpDirectory != null) {
      deleteTempDirectory();
    }
  }

  @Override
  public void afterEach(final ExtensionContext context) {
    if (mongoClient != null) {
      getDatabase().drop();
    }
  }

  @Override
  public void afterAll(final ExtensionContext context) {
    if (mongoClient != null) {
      mongoClient.close();
      mongoClient = null;
    }
    resetSparkContext();
  }

  public String getDatabaseName() {
    String databaseName = getConnectionString().getDatabase();
    return databaseName != null ? databaseName : DEFAULT_DATABASE_NAME;
  }

  public MongoDatabase getDatabase() {
    return getMongoClient().getDatabase(getDatabaseName());
  }

  public String getCollectionName() {
    String collectionName = getConnectionString().getCollection();
    return collectionName != null ? collectionName : DEFAULT_COLLECTION_NAME;
  }

  public ConnectionString getConnectionString() {
    if (connectionString == null) {
      String mongoURIProperty = System.getProperty(URI_SYSTEM_PROPERTY_NAME);
      String mongoURIString =
          mongoURIProperty == null || mongoURIProperty.isEmpty() ? DEFAULT_URI : mongoURIProperty;
      connectionString = new ConnectionString(mongoURIString);
      LOGGER.info(
          "Connecting to: {} : {}", connectionString.getHosts(), connectionString.getCredential());
    }
    return connectionString;
  }

  public SparkConf getSparkConf() {
    return new SparkConf()
        .setMaster("local")
        .setAppName("MongoSparkConnector")
        .set("spark.driver.allowMultipleContexts", "false")
        .set("spark.sql.allowMultipleContexts", "false")
        .set("spark.sql.streaming.checkpointLocation", getTempDirectory())
        .set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .set("spark.app.id", "MongoSparkConnector")
        .set("spark.sql.catalog." + CATALOG, MongoCatalog.class.getCanonicalName())
        .set(
            MongoConfig.PREFIX + MongoConfig.CONNECTION_STRING_CONFIG,
            getConnectionString().getConnectionString())
        .set(MongoConfig.PREFIX + MongoConfig.DATABASE_NAME_CONFIG, getDatabaseName())
        .set(MongoConfig.PREFIX + MongoConfig.COLLECTION_NAME_CONFIG, getCollectionName());
  }

  public SparkContext getOrCreateSparkContext(final SparkConf sparkConfig) {
    return getOrCreateSparkContext(sparkConfig, false);
  }

  public synchronized SparkContext getOrCreateSparkContext(
      final SparkConf sparkConfig, final boolean customConf) {
    boolean createSparkContext = sparkContext == null || customConf;
    if (createSparkContext) {
      resetSparkContext();
      sparkContext = new SparkContext(sparkConfig);
    }
    return sparkContext;
  }

  synchronized void resetSparkContext() {
    if (sparkContext != null) {
      sparkContext.stop();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
    }
    sparkContext = null;
  }

  public boolean isOnline() {
    if (online == null) {
      try {
        getMongoClient().getDatabase("admin").runCommand(BsonDocument.parse("{isMaster: 1}"));
        online = true;
      } catch (Exception e) {
        online = false;
      }
    }
    return online;
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
    loadSampleData(numberOfDocuments, sizeInMB, config, SAMPLE_DATA_TEMPLATE);
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
    loadSampleData(numberOfDocuments, sizeInMB, config, COMPLEX_SAMPLE_DATA_TEMPLATE);
  }

  private void loadSampleData(
      final int numberOfDocuments,
      final int sizeInMB,
      final MongoConfig config,
      final String template) {
    if (!isOnline()) {
      return;
    }
    int sizeBytes = sizeInMB * 1000 * 1000;
    int totalDocumentSize = sizeBytes / numberOfDocuments;
    int sampleDataWithEmptySampleStringSize = RawBsonDocument.parse(
            format(template, "00000", "_10000", "00000", 1, ""))
        .getByteBuffer()
        .limit();
    String sampleString =
        RandomStringUtils.randomAlphabetic(totalDocumentSize - sampleDataWithEmptySampleStringSize);

    List<BsonDocument> sampleDocuments = IntStream.range(0, numberOfDocuments)
        .boxed()
        .map(i -> {
          String idString = StringUtils.leftPad(i.toString(), 5, "0");
          String pkString = format("_%s", i + 10000);
          String dupsString = StringUtils.leftPad(format("%s", i % 3 == 0 ? 0 : i), 5, "0");
          return RawBsonDocument.parse(
              format(template, idString, pkString, dupsString, i % 10, sampleString));
        })
        .collect(Collectors.toList());
    MongoCollection<BsonDocument> coll = getMongoClient()
        .getDatabase(config.getDatabaseName())
        .getCollection(config.getCollectionName(), BsonDocument.class);
    coll.insertMany(sampleDocuments);
  }

  public boolean isSharded() {
    return isOnline() && getClusterType() == ClusterType.SHARDED;
  }

  public ClusterType getClusterType() {
    return getMongoClient().getClusterDescription().getType();
  }

  public void shardCollection(final MongoNamespace mongoNamespace, final String shardKeyJson) {
    if (!isOnline()) {
      return;
    }
    MongoDatabase configDatabase = getMongoClient().getDatabase("config");

    if (configDatabase
            .getCollection("databases")
            .find(Filters.eq("_id", mongoNamespace.getDatabaseName()))
            .first()
        == null) {
      LOGGER.info("Enabling sharding");
      getMongoClient()
          .getDatabase("admin")
          .runCommand(BsonDocument.parse(
              format("{enableSharding: '%s'}", mongoNamespace.getDatabaseName())));

      LOGGER.info("Settings chunkSize to 1MB");
      configDatabase
          .getCollection("settings")
          .updateOne(
              new Document("_id", "chunksize"),
              Updates.set("value", 1),
              new UpdateOptions().upsert(true));
    }

    if (configDatabase
            .getCollection("collections")
            .find(Filters.and(
                Filters.eq("_id", mongoNamespace.getFullName()), Filters.eq("dropped", false)))
            .first()
        == null) {
      LOGGER.info("Sharding: {}", mongoNamespace.getFullName());

      getMongoClient()
          .getDatabase(mongoNamespace.getDatabaseName())
          .createCollection(mongoNamespace.getCollectionName());

      getMongoClient()
          .getDatabase("admin")
          .runCommand(BsonDocument.parse(format(
              "{shardCollection: '%s', key: %s}", mongoNamespace.getFullName(), shardKeyJson)));

      sleep(1000);
    }
  }

  public void sleep(final long timeoutMs) {
    sleep(timeoutMs, null);
  }

  public void sleep(final long timeoutMs, final String message) {
    try {
      Thread.sleep(timeoutMs);
    } catch (InterruptedException interruptedException) {
      if (message != null) {
        fail(message);
      }
    }
  }

  private File createTempDirectory() {
    if (tmpDirectory == null) {
      try {
        File temp = Files.createTempDirectory("mongo-spark-connector").toFile();
        temp.deleteOnExit();
        tmpDirectory = temp;
      } catch (IOException e) {
        throw new UnsupportedOperationException("Could not create a temp directory", e);
      }
    }
    return tmpDirectory;
  }

  public String getTempDirectory() {
    return getTempDirectory(true);
  }

  public String getTempDirectory(final boolean deleteExisting) {
    if (deleteExisting) {
      deleteTempDirectory();
    }
    return createTempDirectory().getAbsolutePath();
  }

  private void deleteTempDirectory() {
    if (tmpDirectory != null) {
      try {
        FileUtils.deleteDirectory(tmpDirectory);
      } catch (IOException e) {
        throw new UnsupportedOperationException("Could not recreate temp directory", e);
      }
      tmpDirectory = null;
    }
  }
}
