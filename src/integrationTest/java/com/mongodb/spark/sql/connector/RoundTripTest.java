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
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

import com.mongodb.spark.sql.connector.beans.BoxedBean;
import com.mongodb.spark.sql.connector.beans.ComplexBean;
import com.mongodb.spark.sql.connector.beans.DateTimeBean;
import com.mongodb.spark.sql.connector.beans.PrimitiveBean;
import com.mongodb.spark.sql.connector.mongodb.MongoSparkConnectorTestCase;
import com.mongodb.spark.sql.connector.write.TruncateMode;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class RoundTripTest extends MongoSparkConnectorTestCase {

  @Test
  void testPrimitiveBean() {
    // Given
    List<PrimitiveBean> dataSetOriginal =
        singletonList(new PrimitiveBean((byte) 1, (short) 2, 3, 4L, 5.0f, 6.0, true));

    // when
    SparkSession spark = getOrCreateSparkSession();
    Encoder<PrimitiveBean> encoder = Encoders.bean(PrimitiveBean.class);

    Dataset<PrimitiveBean> dataset = spark.createDataset(dataSetOriginal, encoder);
    dataset.write().format("mongodb").mode("Overwrite").save();

    // Then
    List<PrimitiveBean> dataSetMongo = spark
        .read()
        .format("mongodb")
        .schema(encoder.schema())
        .load()
        .as(encoder)
        .collectAsList();
    assertIterableEquals(dataSetOriginal, dataSetMongo);
  }

  @ParameterizedTest
  @EnumSource(TruncateMode.class)
  void testBoxedBean(TruncateMode mode) {
    // Given
    List<BoxedBean> dataSetOriginal =
        singletonList(new BoxedBean((byte) 1, (short) 2, 3, 4L, 5.0f, 6.0, true));

    // when
    SparkSession spark = getOrCreateSparkSession();
    Encoder<BoxedBean> encoder = Encoders.bean(BoxedBean.class);

    Dataset<BoxedBean> dataset = spark.createDataset(dataSetOriginal, encoder);
    dataset.write().format("mongodb").mode("Overwrite").option("truncate_mode", mode.name()).save();

    // Then
    List<BoxedBean> dataSetMongo = spark
        .read()
        .format("mongodb")
        .schema(encoder.schema())
        .load()
        .as(encoder)
        .collectAsList();
    assertIterableEquals(dataSetOriginal, dataSetMongo);
  }

  @Test
  void testDateTimeBean() {
    TimeZone original = TimeZone.getDefault();
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.UTC));

      // Given
      long oneHour = TimeUnit.MILLISECONDS.convert(1, TimeUnit.HOURS);
      long oneDay = oneHour * 24;

      List<DateTimeBean> dataSetOriginal = singletonList(new DateTimeBean(
          new Date(oneDay * 365),
          new Timestamp(oneDay + oneHour),
          LocalDate.of(2000, 1, 1),
          Instant.EPOCH));

      // when
      SparkSession spark = getOrCreateSparkSession();
      Encoder<DateTimeBean> encoder = Encoders.bean(DateTimeBean.class);

      Dataset<DateTimeBean> dataset = spark.createDataset(dataSetOriginal, encoder);
      dataset.write().format("mongodb").mode("Overwrite").save();

      // Then
      List<DateTimeBean> dataSetMongo = spark
          .read()
          .format("mongodb")
          .schema(encoder.schema())
          .load()
          .as(encoder)
          .collectAsList();
      assertIterableEquals(dataSetOriginal, dataSetMongo);
    } finally {
      TimeZone.setDefault(original);
    }
  }

  @Test
  void testComplexBean() {
    // Given
    BoxedBean boxedBean = new BoxedBean((byte) 1, (short) 2, 3, 4L, 5.0f, 6.0, true);
    List<String> stringList = asList("a", "b", "c");
    Map<String, String> stringMap = new HashMap<>();
    stringMap.put("a", "a");
    stringMap.put("b", "b");

    List<Map<String, String>> complexList = singletonList(stringMap);
    Map<String, List<String>> complexMap = new HashMap<>();
    complexMap.put("a", stringList);

    List<ComplexBean> dataSetOriginal =
        singletonList(new ComplexBean(boxedBean, stringList, stringMap, complexList, complexMap));

    // when
    SparkSession spark = getOrCreateSparkSession();
    Encoder<ComplexBean> encoder = Encoders.bean(ComplexBean.class);

    Dataset<ComplexBean> dataset = spark.createDataset(dataSetOriginal, encoder);
    dataset.write().format("mongodb").mode("Overwrite").save();

    // Then
    List<ComplexBean> dataSetMongo = spark
        .read()
        .format("mongodb")
        .schema(encoder.schema())
        .load()
        .as(encoder)
        .collectAsList();
    assertIterableEquals(dataSetOriginal, dataSetMongo);
  }
}
