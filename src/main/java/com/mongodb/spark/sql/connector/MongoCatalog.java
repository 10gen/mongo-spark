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

import static java.lang.String.format;
import static java.util.Arrays.asList;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Filters;
import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.config.WriteConfig;
import com.mongodb.spark.sql.connector.exceptions.MongoSparkException;
import com.mongodb.spark.sql.connector.schema.InferSchema;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;

/** Spark Catalog methods for working with namespaces (databases) and tables (collections). */
public class MongoCatalog implements TableCatalog, SupportsNamespaces, SupportsNamespacesAdapter {
  private static final Bson NOT_SYSTEM_NAMESPACE =
      Filters.not(Filters.regex("name", "^system\\..*"));
  private static final Bson IS_COLLECTION =
      Filters.and(NOT_SYSTEM_NAMESPACE, Filters.eq("type", "collection"));

  private boolean initialized;
  private String name;
  private CaseInsensitiveStringMap options;
  private ReadConfig readConfig;
  private WriteConfig writeConfig;

  /**
   * Initializes the MongoCatalog.
   *
   * @param name the name used to identify and load this catalog
   * @param options a case-insensitive string map of configuration
   */
  @Override
  public void initialize(final String name, final CaseInsensitiveStringMap options) {
    Assertions.ensureState(
        () -> !initialized, () -> "The MongoCatalog has already been initialized.");
    initialized = true;
    this.name = name;
    this.options = options;
  }

  /** @return the catalog name */
  @Override
  public String name() {
    assertInitialized();
    return name;
  }

  /**
   * List namespaces (databases).
   *
   * @return an array of namespace (database) names
   */
  @Override
  public String[][] listNamespaces() {
    assertInitialized();
    return filterDatabases(new String[0]);
  }

  /**
   * List namespaces (databases).
   *
   * <p>As MongoDB only supports top level databases, this will return all databases if the database
   * (namespace array) is empty or any empty array if the database exists.
   *
   * @param namespace the optional database array
   * @return an empty array if the database exists
   * @throws NoSuchNamespaceException If the namespace (database) does not exist
   */
  @Override
  public String[][] listNamespaces(final String[] namespace) throws NoSuchNamespaceException {
    assertInitialized();
    if (namespace.length == 0) {
      return listNamespaces();
    } else if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException(namespace);
    }
    return new String[0][];
  }

  /**
   * Currently only returns an empty map if the namespace (database) exists.
   *
   * @param namespace (database) a multi-part namespace
   * @return a string map of properties for the given namespace
   * @throws NoSuchNamespaceException If the namespace (database) does not exist
   */
  @Override
  public Map<String, String> loadNamespaceMetadata(final String[] namespace)
      throws NoSuchNamespaceException {
    assertInitialized();
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException(namespace);
    }
    return Collections.emptyMap();
  }

  /**
   * Test whether a namespace (database) exists.
   *
   * @param namespace (database) a multi-part namespace
   * @return true if the namespace (database) exists, false otherwise
   */
  @Override
  public boolean namespaceExists(final String[] namespace) {
    return filterDatabases(namespace).length > 0;
  }

  /**
   * Create a database.
   *
   * <p>As databases can be created automatically when creating a collection in MongoDB this method
   * only checks to ensure that the database doesn't already exist.
   *
   * @param namespace (database) a multi-part namespace
   * @param metadata a string map of properties for the given namespace
   * @throws NamespaceAlreadyExistsException If the namespace (database) already exists
   * @throws UnsupportedOperationException If create is not a supported operation
   */
  @Override
  public void createNamespace(final String[] namespace, final Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    assertInitialized();
    if (namespaceExists(namespace)) {
      throw new NamespaceAlreadyExistsException(namespace);
    }
  }

  /**
   * Altering databases is currently not supported, so {@code alterNamespace} will always throw an
   * exception.
   *
   * @param namespace (database) a multi-part namespace
   * @param changes a collection of changes to apply to the namespace
   * @throws UnsupportedOperationException If namespace (database) properties are not supported
   */
  @Override
  public void alterNamespace(final String[] namespace, final NamespaceChange... changes) {
    assertInitialized();
    throw new UnsupportedOperationException("Altering databases is currently not supported");
  }

  @Override
  public boolean dropNamespace(final String[] namespace, final boolean cascade) {
    return dropNamespace(namespace);
  }

  @Override
  public boolean dropNamespace(final String[] namespace) {
    assertInitialized();
    if (namespaceExists(namespace)) {
      MongoConfig.writeConfig(options)
          .doWithClient(c -> c.getDatabase(namespace[0]).drop());
      return true;
    }
    return false;
  }

  /**
   * List the collections in a namespace (database).
   *
   * <p>Note: Will only return collections and not views.
   *
   * @param namespace (database) a multi-part namespace
   * @return an array of Identifiers for tables (collections)
   */
  @Override
  public Identifier[] listTables(final String[] namespace) {
    return filterCollections(Identifier.of(namespace, ""));
  }

  /**
   * Test whether a collection exists.
   *
   * @param identifier a collection identifier
   * @return true if the collection exists, false otherwise
   */
  @Override
  public boolean tableExists(final Identifier identifier) {
    assertInitialized();
    if (identifier.namespace().length != 1) {
      return false;
    }
    return asList(listTables(identifier.namespace())).contains(identifier);
  }

  /**
   * Load collection.
   *
   * @param identifier a collection identifier
   * @return the table's metadata
   * @throws NoSuchTableException If the collection doesn't exist or is a view
   */
  @Override
  public Table loadTable(final Identifier identifier) throws NoSuchTableException {
    assertInitialized();
    if (!tableExists(identifier)) {
      throw new NoSuchTableException(identifier);
    }
    Map<String, String> properties = new HashMap<>(options);
    properties.put(
        MongoConfig.READ_PREFIX + MongoConfig.DATABASE_NAME_CONFIG, identifier.namespace()[0]);
    properties.put(MongoConfig.READ_PREFIX + MongoConfig.COLLECTION_NAME_CONFIG, identifier.name());
    return new MongoTable(InferSchema.inferSchema(new CaseInsensitiveStringMap(properties)), MongoConfig.readConfig(properties));
  }

  /**
   * Create a collection.
   *
   * @param identifier a collection identifier
   * @param schema the schema of the new table, as a struct type
   * @param partitions transforms to use for partitioning data in the table
   * @param properties a string map of collection properties
   * @return metadata for the new table
   * @throws TableAlreadyExistsException If a collection or view already exists for the identifier
   * @throws UnsupportedOperationException If a requested partition transform is not supported
   */
  @Override
  public Table createTable(
      final Identifier identifier,
      final StructType schema,
      final Transform[] partitions,
      final Map<String, String> properties)
      throws TableAlreadyExistsException {
    assertInitialized();
    if (identifier.namespace().length != 1) {
      throw new UnsupportedOperationException(
          format("Invalid namespace: %s", String.join(",", identifier.namespace())));
    }
    if (tableExists(identifier)) {
      throw new TableAlreadyExistsException(identifier);
    } else if (partitions.length > 0) {
      throw new UnsupportedOperationException("Cannot create MongoDB collection with partitions");
    } else if (!properties.isEmpty()) {
      // TODO - SPARK-309 support create collection options (prefixed by TableCatalog.OPTION_PREFIX)
      throw new UnsupportedOperationException(format(
          "MongoCatalog.createTable does not support the following options: %s",
          String.join(",", properties.keySet())));
    }

    getWriteConfig().doWithClient(c -> c.getDatabase(identifier.namespace()[0])
        .createCollection(identifier.name()));
    return new MongoTable(schema, getWriteConfig());
  }

  /**
   * Altering collections is currently not supported, so {@code alterTable} will always throw an
   * exception.
   *
   * @param identifier a collection identifier
   * @param changes changes to apply to the table
   * @return will throw an exception as altering collections is not supported.
   * @throws NoSuchTableException If the collection doesn't exist or is a view
   * @throws IllegalArgumentException If any change is rejected by the implementation.
   */
  @Override
  public Table alterTable(final Identifier identifier, final TableChange... changes)
      throws NoSuchTableException {
    assertInitialized();
    if (!tableExists(identifier)) {
      throw new NoSuchTableException(identifier);
    }
    throw new IllegalArgumentException("Altering collections is not supported.");
  }

  /**
   * Drop a collection.
   *
   * @param identifier a collection identifier
   * @return true if a collection was deleted, false if no collection exists for the identifier
   */
  @Override
  public boolean dropTable(final Identifier identifier) {
    assertInitialized();
    if (identifier.namespace().length == 1 && filterCollections(identifier).length != 0) {
      getWriteConfig().doWithClient(c -> c.getDatabase(identifier.namespace()[0])
          .getCollection(identifier.name())
          .drop());
      return true;
    }
    return false;
  }

  /**
   * Renames a collection.
   *
   * @param oldIdentifier the collection identifier of the existing collection to rename
   * @param newIdentifier the new collection identifier of the table
   * @throws NoSuchTableException If the collection to rename doesn't exist or is a view
   * @throws TableAlreadyExistsException If the new collection name already exists or is a view
   */
  @Override
  public void renameTable(final Identifier oldIdentifier, final Identifier newIdentifier)
      throws NoSuchTableException, TableAlreadyExistsException {
    if (!tableExists(oldIdentifier)) {
      throw new NoSuchTableException(oldIdentifier);
    } else if (tableExists(newIdentifier)) {
      throw new TableAlreadyExistsException(newIdentifier);
    }

    try {
      getWriteConfig().doWithClient(c -> c.getDatabase(oldIdentifier.namespace()[0])
          .getCollection(oldIdentifier.name())
          .renameCollection(
              new MongoNamespace(newIdentifier.namespace()[0], newIdentifier.name())));
    } catch (MongoCommandException ex) {
      throw new MongoSparkException("Unable to rename table due to: " + ex.getErrorMessage(), ex);
    }
  }

  private void assertInitialized() {
    Assertions.ensureState(() -> initialized, () -> "The MongoCatalog has not been initialized.");
  }

  private String[][] filterDatabases(final String[] databaseName) {
    assertInitialized();
    if (databaseName.length > 1) {
      return new String[0][];
    }
    Bson filter = databaseName.length == 0
        ? NOT_SYSTEM_NAMESPACE
        : Filters.and(Filters.eq("name", databaseName[0]), NOT_SYSTEM_NAMESPACE);

    return getReadConfig().withClient(client -> client
        .listDatabases()
        .filter(filter)
        .nameOnly(true)
        .map(d -> new String[] {d.getString("name")})
        .into(new ArrayList<>())
        .toArray(new String[0][]));
  }

  private Identifier[] filterCollections(final Identifier identifier) {
    assertInitialized();
    Assertions.ensureArgument(
        () -> identifier.namespace().length == 1, () -> "Namespace size must equal 1");

    Bson filter = identifier.name().isEmpty()
        ? IS_COLLECTION
        : Filters.and(Filters.eq("name", identifier.name()), IS_COLLECTION);

    return getReadConfig().withClient(c -> c.getDatabase(identifier.namespace()[0])
        .listCollections()
        .filter(filter)
        .map(d -> Identifier.of(identifier.namespace(), d.getString("name")))
        .into(new ArrayList<>())
        .toArray(new Identifier[0]));
  }

  private ReadConfig getReadConfig() {
    assertInitialized();
    if (readConfig == null) {
      readConfig = MongoConfig.readConfig(options);
    }
    return readConfig;
  }

  @VisibleForTesting
  WriteConfig getWriteConfig() {
    assertInitialized();
    if (writeConfig == null) {
      writeConfig = MongoConfig.writeConfig(options);
    }
    return writeConfig;
  }

  @TestOnly
  @VisibleForTesting
  void reset(final Runnable onReset) {
    if (initialized) {
      onReset.run();
      initialized = false;
      name = null;
      options = null;
      readConfig = null;
      writeConfig = null;
    }
  }
}
