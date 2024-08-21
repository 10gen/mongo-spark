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

package com.mongodb.spark.sql.connector.schema;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import com.mongodb.spark.sql.connector.config.WriteConfig;
import com.mongodb.spark.sql.connector.exceptions.DataException;
import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;
import com.mongodb.spark.sql.connector.interop.JavaScala;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.json.JsonParseException;
import org.bson.types.Decimal128;
import org.jetbrains.annotations.NotNull;

/**
 * The helper for conversion of GenericRowWithSchema instances to BsonDocuments.
 *
 * <p>All Spark types are considered convertible to Bson types.
 */
@NotNull
public final class RowToBsonDocumentConverter implements Serializable {

  private static final long serialVersionUID = 1L;

  private final InternalRowToRowFunction internalRowToRowFunction;
  private final boolean ignoreNulls;
  private final List<ObjectToBsonElement> mappers;

  /**
   * Construct a new instance
   *
   * @param schema the schema for the row
   * @param convertJson the convert Json configuration
   * @param ignoreNulls if true ignore any null values, even those in arrays, maps or struct values
   */
  public RowToBsonDocumentConverter(
      final StructType schema,
      final WriteConfig.ConvertJson convertJson,
      final boolean ignoreNulls) {

    this.internalRowToRowFunction = createInternalRowToRowFunction(schema);
    this.ignoreNulls = ignoreNulls;
    this.mappers = Arrays.stream(schema.fields())
        .map(f -> dataTypeToBsonElement(f, convertJson, ignoreNulls))
        .collect(Collectors.toList());
  }

  /**
   * Converts a {@link InternalRow} to a {@link BsonDocument}
   *
   * @param row the internal row to convert
   * @throws DataException if the {@code Row} is not convertable to a {@code BsonDocument}
   * @return a BsonDocument representing the data in the row
   */
  public BsonDocument fromRow(final InternalRow row) {
    return fromRow(internalRowToRowFunction.apply(row));
  }

  /**
   * Converts a {@link Row} to a {@link BsonDocument}
   *
   * @param row the row to convert
   * @throws DataException if the {@code Row} is not convertable to a {@code BsonDocument}
   * @return a BsonDocument representing the data in the row
   */
  public BsonDocument fromRow(final Row row) {
    return rowToBsonDocument(row, mappers, ignoreNulls);
  }

  /**
   * Returns a conversion function that converts an object to a BsonValue
   *
   * @param dataType the data type of the object
   * @param convertJson the string json conversion setting
   * @param ignoreNulls true if ignoring null values
   * @return a serializable function converts an object to a BsonValue
   */
  public static ObjectToBsonValue createObjectToBsonValue(
      final DataType dataType,
      final WriteConfig.ConvertJson convertJson,
      final boolean ignoreNulls) {
    ObjectToBsonValue cachedObjectToBsonValue =
        objectToBsonValue(dataType, convertJson, ignoreNulls);
    return (data) -> {
      if (!ignoreNulls && data == null) {
        return BsonNull.VALUE;
      }
      try {
        return cachedObjectToBsonValue.apply(data);
      } catch (Exception e) {
        throw new DataException(format(
            "Cannot cast %s into a BsonValue. %s has no matching BsonValue. Error: %s",
            data, dataType, e.getMessage()), e);
      }
    };
  }

  private static ObjectToBsonElement dataTypeToBsonElement(
      final StructField field,
      final WriteConfig.ConvertJson convertJson,
      final boolean ignoreNulls) {
    ObjectToBsonValue cachedObjectToBsonValue =
        createObjectToBsonValue(field.dataType(), convertJson, ignoreNulls);
    return (data) -> new BsonElement(field.name(), cachedObjectToBsonValue.apply(data));
  }

  @SuppressWarnings("unchecked")
  private static ObjectToBsonValue objectToBsonValue(
      final DataType dataType,
      final WriteConfig.ConvertJson convertJson,
      final boolean ignoreNulls) {
    if (DataTypes.BinaryType.acceptsType(dataType)) {
      return (data) -> new BsonBinary((byte[]) data);
    } else if (DataTypes.BooleanType.acceptsType(dataType)) {
      return (data) -> new BsonBoolean((Boolean) data);
    } else if (DataTypes.DoubleType.acceptsType(dataType)) {
      return (data) -> new BsonDouble(((Number) data).doubleValue());
    } else if (DataTypes.FloatType.acceptsType(dataType)) {
      return (data) -> new BsonDouble(((Number) data).floatValue());
    } else if (DataTypes.IntegerType.acceptsType(dataType)) {
      return (data) -> new BsonInt32(((Number) data).intValue());
    } else if (DataTypes.ShortType.acceptsType(dataType)) {
      return (data) -> new BsonInt32(((Number) data).intValue());
    } else if (DataTypes.ByteType.acceptsType(dataType)) {
      return (data) -> new BsonInt32(((Number) data).intValue());
    } else if (DataTypes.LongType.acceptsType(dataType)) {
      return (data) -> new BsonInt64(((Number) data).longValue());
    } else if (DataTypes.StringType.acceptsType(dataType)) {
      return (data) -> processString((String) data, convertJson);
    } else if (DataTypes.DateType.acceptsType(dataType)
        || DataTypes.TimestampType.acceptsType(dataType)
        || DataTypes.TimestampNTZType.acceptsType(dataType)
    ) {
      return (data) -> {
        if (data instanceof Date) {
          // Covers java.util.Date, java.sql.Date, java.sql.Timestamp
          return new BsonDateTime(((Date) data).getTime());
        }
        if (data instanceof Instant) {
            return new BsonDateTime(((Instant) data).toEpochMilli());
        }
        if (data instanceof LocalDateTime) {
          LocalDateTime dateTime = (LocalDateTime) data;
          return new BsonDateTime(Timestamp.valueOf(dateTime).getTime());
        }
        if (data instanceof LocalDate) {
          long epochSeconds = ((LocalDate) data).toEpochDay() * 24L * 3600L;
          return new BsonDateTime(epochSeconds * 1000L);
        }

        /*
        NOTE 1: ZonedDateTime, OffsetDateTime, OffsetTime are not explicitly supported by Spark and causing Encoder resolver to fail due to
         cyclic dependency in the ZoneOffset. Subject for review after it changes (if ever).
        NOTE 2: LocalTime type is not represented neither in Bson nor in Spark
         */

        throw new MongoSparkException("Unsupported date type: " + data.getClass().getSimpleName());
      };
    } else if (DataTypes.NullType.acceptsType(dataType)) {
      return (data) -> BsonNull.VALUE;
    } else if (dataType instanceof DecimalType) {
      return (data) -> {
        BigDecimal bigDecimal = data instanceof BigDecimal
            ? (BigDecimal) data
            : ((Decimal) data).toBigDecimal().bigDecimal();
        return new BsonDecimal128(new Decimal128(bigDecimal));
      };
    } else if (dataType instanceof ArrayType) {
      DataType elementType = ((ArrayType) dataType).elementType();
      Function<Object, BsonValue> element =
          createObjectToBsonValue(elementType, convertJson, ignoreNulls);
      return (data) -> {
        BsonArray bsonArray = new BsonArray();

        List<Object> listData;
        if (data instanceof List) {
          listData = (List<Object>) data;
        } else if (data instanceof Object[]) {
          listData = asList((Object[]) data);
        } else {
          listData = JavaScala.asJava((scala.collection.Seq<Object>) data);
        }
        for (Object obj : listData) {
          if (!(ignoreNulls && Objects.isNull(obj))) {
            bsonArray.add(element.apply(obj));
          }
        }
        return bsonArray;
      };
    } else if (dataType instanceof MapType) {
      DataType keyType = ((MapType) dataType).keyType();
      DataType valueType = ((MapType) dataType).valueType();
      if (!(keyType instanceof StringType)) {
        throw new DataException(
            format("Cannot cast Map into a BsonValue. Invalid key type %s.", keyType));
      }
      Function<Object, BsonValue> value =
          createObjectToBsonValue(valueType, convertJson, ignoreNulls);

      return (data) -> {
        BsonDocument bsonDocument = new BsonDocument();
        Map<String, Object> mapData;
        if (data instanceof Map) {
          mapData = (Map<String, Object>) data;
        } else {
          mapData = JavaScala.asJava((scala.collection.Map<String, Object>) data);
        }
        for (Map.Entry<String, Object> entry : mapData.entrySet()) {
          if (!(ignoreNulls && Objects.isNull(entry.getValue()))) {
            bsonDocument.put(entry.getKey(), value.apply(entry.getValue()));
          }
        }
        return bsonDocument;
      };
    } else if (dataType instanceof StructType) {
      List<ObjectToBsonElement> local = Arrays.stream(((StructType) dataType).fields())
          .map(f -> dataTypeToBsonElement(f, convertJson, ignoreNulls))
          .collect(Collectors.toList());
      return (data) -> {
        Row row = (Row) data;
        return rowToBsonDocument(row, local, ignoreNulls);
      };
    }

    return (data) -> {
      throw new DataException(format(
          "Cannot cast %s into a BsonValue. %s data type has no matching BsonValue.",
          data, dataType));
    };
  }

    private static BsonDocument rowToBsonDocument(
      final Row row,
      final List<ObjectToBsonElement> objectToBsonElements,
      final boolean ignoreNulls) {
    BsonDocument bsonDocument = new BsonDocument();
    for (int i = 0; i < objectToBsonElements.size(); i++) {
      Object value = row.get(i);
      if (!(ignoreNulls && Objects.isNull(value))) {
        BsonElement bsonElement = objectToBsonElements.get(i).apply(value);
        bsonDocument.append(bsonElement.getName(), bsonElement.getValue());
      }
    }
    return bsonDocument;
  }

  private static final String BSON_TEMPLATE = "{v: %s}";
  private static final char JSON_OBJECT_START = '{';
  private static final char JSON_OBJECT_END = '}';

  private static final char JSON_ARRAY_START = '[';
  private static final char JSON_ARRAY_END = ']';

  private static boolean isJsonObjectOrArray(final String data) {
    if (data.isEmpty()) {
      return false;
    }
    char firstChar = data.charAt(0);
    char lastChar = data.charAt(data.length() - 1);
    return (firstChar == JSON_OBJECT_START && lastChar == JSON_OBJECT_END)
        || (firstChar == JSON_ARRAY_START && lastChar == JSON_ARRAY_END);
  }

  private static BsonValue processString(
      final String data, final WriteConfig.ConvertJson convertJson) {
    if (parseJsonData(data, convertJson)) {
      try {
        return BsonDocument.parse(format(BSON_TEMPLATE, data)).get("v");
      } catch (JsonParseException e) {
        return new BsonString(data);
      }
    }
    return new BsonString(data);
  }

  private static boolean parseJsonData(
      final String data, final WriteConfig.ConvertJson convertJson) {
    switch (convertJson) {
      case FALSE:
        return false;
      case ANY:
        return true;
      case OBJECT_OR_ARRAY_ONLY:
        return isJsonObjectOrArray(data);
      default:
        throw new AssertionError("Unexpected value: " + convertJson);
    }
  }

  private static InternalRowToRowFunction createInternalRowToRowFunction(final StructType schema) {
    try {
      return new InternalRowToRowFunction(schema);
    } catch (Exception e) {
      throw new DataException(
          format(
              "Unable to create InternalRow to Row Function using %s. Errors: %s",
              schema, e.getMessage()),
          e);
    }
  }

  interface ObjectToBsonElement extends Function<Object, BsonElement>, Serializable {}

  /**
   * A serializable {@code Function<Object, BsonValue>} interface.
   */
  public interface ObjectToBsonValue extends Function<Object, BsonValue>, Serializable {}
}
