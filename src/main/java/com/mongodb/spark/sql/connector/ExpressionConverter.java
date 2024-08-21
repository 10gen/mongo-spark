package com.mongodb.spark.sql.connector;

import static com.mongodb.spark.sql.connector.schema.RowToBsonDocumentConverter.createObjectToBsonValue;
import static java.lang.String.format;

import com.mongodb.client.model.Filters;
import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.config.WriteConfig;
import com.mongodb.spark.sql.connector.schema.RowToBsonDocumentConverter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualNullSafe;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.sources.StringStartsWith;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

public class ExpressionConverter {
  private final StructType schema;

  public ExpressionConverter(final StructType schema) {
    this.schema = schema;
  }

  public FilterAndPipelineStage processFilter(final Filter filter) {
    Assertions.ensureArgument(() -> filter != null, () -> "Invalid argument filter cannot be null");
    if (filter instanceof And) {
      And andFilter = (And) filter;
      FilterAndPipelineStage eitherLeft = processFilter(andFilter.left());
      FilterAndPipelineStage eitherRight = processFilter(andFilter.right());
      if (eitherLeft.hasPipelineStage() && eitherRight.hasPipelineStage()) {
        return new FilterAndPipelineStage(
            filter, Filters.and(eitherLeft.getPipelineStage(), eitherRight.getPipelineStage()));
      }
    } else if (filter instanceof EqualNullSafe) {
      EqualNullSafe equalNullSafe = (EqualNullSafe) filter;
      String fieldName = unquoteFieldName(equalNullSafe.attribute());
      return new FilterAndPipelineStage(
          filter,
          getBsonValue(fieldName, equalNullSafe.value())
              .map(bsonValue -> Filters.eq(fieldName, bsonValue))
              .orElse(null));
    } else if (filter instanceof EqualTo) {
      EqualTo equalTo = (EqualTo) filter;
      String fieldName = unquoteFieldName(equalTo.attribute());
      return new FilterAndPipelineStage(
          filter,
          getBsonValue(fieldName, equalTo.value())
              .map(bsonValue -> Filters.eq(fieldName, bsonValue))
              .orElse(null));
    } else if (filter instanceof GreaterThan) {
      GreaterThan greaterThan = (GreaterThan) filter;
      String fieldName = unquoteFieldName(greaterThan.attribute());
      return new FilterAndPipelineStage(
          filter,
          getBsonValue(fieldName, greaterThan.value())
              .map(bsonValue -> Filters.gt(fieldName, bsonValue))
              .orElse(null));
    } else if (filter instanceof GreaterThanOrEqual) {
      GreaterThanOrEqual greaterThanOrEqual = (GreaterThanOrEqual) filter;
      String fieldName = unquoteFieldName(greaterThanOrEqual.attribute());
      return new FilterAndPipelineStage(
          filter,
          getBsonValue(fieldName, greaterThanOrEqual.value())
              .map(bsonValue -> Filters.gte(fieldName, bsonValue))
              .orElse(null));
    } else if (filter instanceof In) {
      In inFilter = (In) filter;
      String fieldName = unquoteFieldName(inFilter.attribute());
      List<BsonValue> values =
          Arrays.stream(inFilter.values())
              .map(v -> getBsonValue(fieldName, v))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .collect(Collectors.toList());

      // Ensure all values were matched otherwise leave to Spark to filter.
      Bson pipelineStage = null;
      if (values.size() == inFilter.values().length) {
        pipelineStage = Filters.in(fieldName, values);
      }
      return new FilterAndPipelineStage(filter, pipelineStage);
    } else if (filter instanceof IsNull) {
      IsNull isNullFilter = (IsNull) filter;
      String fieldName = unquoteFieldName(isNullFilter.attribute());
      return new FilterAndPipelineStage(filter, Filters.eq(fieldName, null));
    } else if (filter instanceof IsNotNull) {
      IsNotNull isNotNullFilter = (IsNotNull) filter;
      String fieldName = unquoteFieldName(isNotNullFilter.attribute());
      return new FilterAndPipelineStage(filter, Filters.ne(fieldName, null));
    } else if (filter instanceof LessThan) {
      LessThan lessThan = (LessThan) filter;
      String fieldName = unquoteFieldName(lessThan.attribute());
      return new FilterAndPipelineStage(
          filter,
          getBsonValue(fieldName, lessThan.value())
              .map(bsonValue -> Filters.lt(fieldName, bsonValue))
              .orElse(null));
    } else if (filter instanceof LessThanOrEqual) {
      LessThanOrEqual lessThanOrEqual = (LessThanOrEqual) filter;
      String fieldName = unquoteFieldName(lessThanOrEqual.attribute());
      return new FilterAndPipelineStage(
          filter,
          getBsonValue(fieldName, lessThanOrEqual.value())
              .map(bsonValue -> Filters.lte(fieldName, bsonValue))
              .orElse(null));
    } else if (filter instanceof Not) {
      Not notFilter = (Not) filter;
      FilterAndPipelineStage notChild = processFilter(notFilter.child());
      if (notChild.hasPipelineStage()) {
        return new FilterAndPipelineStage(filter, Filters.not(notChild.pipelineStage));
      }
    } else if (filter instanceof Or) {
      Or or = (Or) filter;
      FilterAndPipelineStage eitherLeft = processFilter(or.left());
      FilterAndPipelineStage eitherRight = processFilter(or.right());
      if (eitherLeft.hasPipelineStage() && eitherRight.hasPipelineStage()) {
        return new FilterAndPipelineStage(
            filter, Filters.or(eitherLeft.getPipelineStage(), eitherRight.getPipelineStage()));
      }
    } else if (filter instanceof StringContains) {
      StringContains stringContains = (StringContains) filter;
      String fieldName = unquoteFieldName(stringContains.attribute());
      return new FilterAndPipelineStage(
          filter, Filters.regex(fieldName, format(".*%s.*", stringContains.value())));
    } else if (filter instanceof StringEndsWith) {
      StringEndsWith stringEndsWith = (StringEndsWith) filter;
      String fieldName = unquoteFieldName(stringEndsWith.attribute());
      return new FilterAndPipelineStage(
          filter, Filters.regex(fieldName, format(".*%s$", stringEndsWith.value())));
    } else if (filter instanceof StringStartsWith) {
      StringStartsWith stringStartsWith = (StringStartsWith) filter;
      String fieldName = unquoteFieldName(stringStartsWith.attribute());
      return new FilterAndPipelineStage(
          filter, Filters.regex(fieldName, format("^%s.*", stringStartsWith.value())));
    }
    return new FilterAndPipelineStage(filter, null);
  }

  @VisibleForTesting
  static String unquoteFieldName(final String fieldName) {
    // Spark automatically escapes hyphenated names using backticks
    if (fieldName.contains("`")) {
      return new Column(fieldName).toString();
    }
    return fieldName;
  }

  private Optional<BsonValue> getBsonValue(final String fieldName, final Object value) {
    try {
      StructType localSchema = schema;
      DataType localDataType = localSchema;

      for (String localFieldName : fieldName.split("\\.")) {
        StructField localField = localSchema.apply(localFieldName);
        localDataType = localField.dataType();
        if (localField.dataType() instanceof StructType) {
          localSchema = (StructType) localField.dataType();
        }
      }
      RowToBsonDocumentConverter.ObjectToBsonValue objectToBsonValue =
          createObjectToBsonValue(localDataType, WriteConfig.ConvertJson.FALSE, false);
      return Optional.of(objectToBsonValue.apply(value));
    } catch (Exception e) {
      // ignore
      return Optional.empty();
    }
  }

  /** FilterAndPipelineStage - contains an optional pipeline stage for the filter. */
  public static final class FilterAndPipelineStage {

    private final Filter filter;
    private final Bson pipelineStage;

    private FilterAndPipelineStage(final Filter filter, @Nullable final Bson pipelineStage) {
      this.filter = filter;
      this.pipelineStage = pipelineStage;
    }

    public Filter getFilter() {
      return filter;
    }

    public Bson getPipelineStage() {
      return pipelineStage;
    }

    public boolean hasPipelineStage() {
      return pipelineStage != null;
    }
  }
}
