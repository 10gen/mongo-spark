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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.spark.sql.connector.ExpressionConverter;
import com.mongodb.spark.sql.connector.ExpressionConverter.FilterAndPipelineStage;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.BsonDocument;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.VisibleForTesting;

/** A builder for a {@link MongoScan}. */
@ApiStatus.Internal
public final class MongoScanBuilder
    implements
        ScanBuilder,
        SupportsPushDownFilters,
        SupportsPushDownRequiredColumns {
  private final StructType schema;
  private final ReadConfig readConfig;
  private final boolean isCaseSensitive;
  private List<BsonDocument> datasetAggregationPipeline;
  private Filter[] pushedFilters;
  private StructType prunedSchema;

  /**
   * Construct a new instance
   *
   * @param schema the schema to use for the read
   * @param readConfig the configuration for the read
   */
  public MongoScanBuilder(final StructType schema, final ReadConfig readConfig) {
    this.schema = schema;
    this.readConfig = readConfig;
    this.prunedSchema = schema;
    this.isCaseSensitive = SparkSession.getActiveSession()
        .map(s -> s.sessionState().conf().caseSensitiveAnalysis())
        .getOrElse(() -> false);
    this.datasetAggregationPipeline = emptyList();
    this.pushedFilters = new Filter[0];
  }

  /** @return the {@link MongoScan} for the configured scan */
  @Override
  public Scan build() {
    List<BsonDocument> scanAggregationPipeline = new ArrayList<>();
    scanAggregationPipeline.addAll(readConfig.getAggregationPipeline());
    scanAggregationPipeline.addAll(datasetAggregationPipeline);

    ReadConfig scanReadConfig = readConfig.withOption(
        MongoConfig.READ_PREFIX + ReadConfig.AGGREGATION_PIPELINE_CONFIG,
        scanAggregationPipeline.stream()
            .map(BsonDocument::toJson)
            .collect(Collectors.joining(",", "[", "]")));
    return new MongoScan(prunedSchema, scanReadConfig);
  }

  /**
   * Processes filters on the dataset.
   *
   * <p>Sets any filters that can be pushed down into an aggregation `$match` pipeline stage.
   *
   * @param filters data filters
   * @return any filters for Spark to process
   */
  @Override
  public Filter[] pushFilters(final Filter[] filters) {
      ExpressionConverter converter = new ExpressionConverter(schema);

    List<FilterAndPipelineStage> processed =
        Arrays.stream(filters).map(converter::processFilter).collect(Collectors.toList());

    List<FilterAndPipelineStage> withPipelines = processed.stream()
        .filter(FilterAndPipelineStage::hasPipelineStage)
        .collect(Collectors.toList());

    datasetAggregationPipeline = withPipelines.isEmpty()
        ? emptyList()
        : singletonList(Aggregates.match(Filters.and(withPipelines.stream()
                .map(FilterAndPipelineStage::getPipelineStage)
                .collect(Collectors.toList())))
            .toBsonDocument());
    pushedFilters =
        withPipelines.stream().map(FilterAndPipelineStage::getFilter).toArray(Filter[]::new);

    return processed.stream()
        .filter(e -> !e.hasPipelineStage())
        .map(FilterAndPipelineStage::getFilter)
        .toArray(Filter[]::new);
  }

  /** @return any filters that have been converted into an aggregation pipeline. */
  @Override
  public Filter[] pushedFilters() {
    return pushedFilters;
  }

  @Override
  public void pruneColumns(final StructType requiredSchema) {
    Set<String> requiredColumns =
        Arrays.stream(requiredSchema.fields()).map(this::getColumnName).collect(Collectors.toSet());
    StructField[] fields = Arrays.stream(schema.fields())
        .filter(f -> requiredColumns.contains(getColumnName(f)))
        .toArray(StructField[]::new);
    prunedSchema = new StructType(fields);
  }

  private String getColumnName(final StructField field) {
    if (isCaseSensitive) {
      return field.name().toLowerCase(Locale.ROOT);
    }
    return field.name();
  }

  @VisibleForTesting
  static String unquoteFieldName(final String fieldName) {
    // Spark automatically escapes hyphenated names using backticks
    if (fieldName.contains("`")) {
      return new Column(fieldName).toString();
    }
    return fieldName;
  }
}
