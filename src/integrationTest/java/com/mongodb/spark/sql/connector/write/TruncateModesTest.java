package com.mongodb.spark.sql.connector.write;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.spark.sql.connector.beans.BoxedBean;
import com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorTestCase;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class TruncateModesTest extends MongoSparkConnectorTestCase {

  public static final String INT_FIELD_INDEX = "intFieldIndex";
  public static final String ID_INDEX = "_id_";

  //  private static final Encoder<BoxedBean> encoder = Encoders.bean(BoxedBean.class);

  @BeforeEach
  void setup() {
    MongoDatabase database = getDatabase();
    getCollection().drop();
    CreateCollectionOptions createCollectionOptions =
        new CreateCollectionOptions().capped(true).maxDocuments(1024).sizeInBytes(1024);
    database.createCollection(getCollectionName(), createCollectionOptions);
    MongoCollection<Document> collection = database.getCollection(getCollectionName());
    collection.insertOne(new Document().append("intField", null));
    collection.createIndex(
        new Document().append("intField", 1), new IndexOptions().name(INT_FIELD_INDEX));
  }

  @Test
  void testCollectionDroppedOnOverwrite() {
    // Given
    List<BoxedBean> dataSetOriginal = singletonList(getBoxedBean());

    // when
    SparkSession spark = getOrCreateSparkSession();
    Encoder<BoxedBean> encoder = Encoders.bean(BoxedBean.class);
    Dataset<BoxedBean> dataset = spark.createDataset(dataSetOriginal, encoder);
    dataset
        .write()
        .format("mongodb")
        .mode("Overwrite")
        .option("truncate_mode", TruncateMode.DROP.name())
        .save();

    // Then
    List<BoxedBean> dataSetMongo =
        spark.read().format("mongodb").schema(encoder.schema()).load().as(encoder).collectAsList();
    assertIterableEquals(dataSetOriginal, dataSetMongo);

    List<String> indexes =
        getCollection().listIndexes().map(it -> it.getString("name")).into(new ArrayList<>());
    assertEquals(indexes, singletonList(ID_INDEX));
    Document options = getCollectionOptions();
    assertTrue(options.isEmpty());
  }

  @ParameterizedTest
  @EnumSource(
      value = TruncateMode.class,
      names = {"DROP"},
      mode = EnumSource.Mode.EXCLUDE)
  void testOptionKeepingOverwrites(TruncateMode mode) {
    // Given
    List<BoxedBean> dataSetOriginal = singletonList(getBoxedBean());

    // when
    SparkSession spark = getOrCreateSparkSession();
    Encoder<BoxedBean> encoder = Encoders.bean(BoxedBean.class);
    Dataset<BoxedBean> dataset = spark.createDataset(dataSetOriginal, encoder);
    dataset.write().format("mongodb").mode("Overwrite").option("truncate_mode", mode.name()).save();

    // Then
    List<BoxedBean> dataSetMongo =
        spark.read().format("mongodb").schema(encoder.schema()).load().as(encoder).collectAsList();
    assertIterableEquals(dataSetOriginal, dataSetMongo);

    List<String> indexes =
        getCollection().listIndexes().map(it -> it.getString("name")).into(new ArrayList<>());
    assertEquals(indexes, asList(ID_INDEX, INT_FIELD_INDEX));

    Document options = getCollectionOptions();
    assertTrue(options.getBoolean("capped"));
  }

  private @NotNull BoxedBean getBoxedBean() {
    return new BoxedBean((byte) 1, (short) 2, 3, 4L, 5.0f, 6.0, true);
  }

  private Document getCollectionOptions() {
    Document getCollectionMeta =
        new Document()
            .append("listCollections", 1)
            .append("filter", new Document().append("name", getCollectionName()));

    Document foundMeta = getDatabase().runCommand(getCollectionMeta);
    Document cursor = foundMeta.get("cursor", Document.class);
    List<Document> firstBatch = cursor.getList("firstBatch", Document.class);
    if (firstBatch.isEmpty()) {
      return getCollectionMeta;
    }

    return firstBatch.get(0).get("options", Document.class);
  }
}
