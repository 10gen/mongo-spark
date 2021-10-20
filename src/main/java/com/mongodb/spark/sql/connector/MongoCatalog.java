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
import org.jetbrains.annotations.VisibleForTesting;

import org.bson.conversions.Bson;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Filters;

import com.mongodb.spark.sql.connector.assertions.Assertions;
import com.mongodb.spark.sql.connector.config.MongoConfig;
import com.mongodb.spark.sql.connector.config.ReadConfig;
import com.mongodb.spark.sql.connector.config.WriteConfig;

/** Spark Catalog methods for working with namespaces (databases) and tables (collections). */
public class MongoCatalog implements TableCatalog, SupportsNamespaces {
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
   * Called to initialize configuration.
   *
   * <p>This method is called once, just after the provider is instantiated.
   *
   * @param name the name used to identify and load this catalog
   * @param options a case-insensitive string map of configuration
   */
  @Override
  public void initialize(final String name, final CaseInsensitiveStringMap options) {
    Assertions.ensureState(() -> !initialized, "The MongoCatalog has already been initialized.");
    initialized = true;
    this.name = name;
    this.options = options;
  }

  /**
   * Called to get this catalog's name.
   *
   * <p>This method is only called after {@link #initialize(String, CaseInsensitiveStringMap)} is
   * called to pass the catalog's name.
   */
  @Override
  public String name() {
    assertInitialized();
    return name;
  }

  /**
   * List top-level namespaces (databases) from the catalog.
   *
   * <p>MongoDB only has top level namespaces (databases) and does not support nested databases.
   *
   * @return an array of multi-part namespace (database) names
   */
  @Override
  public String[][] listNamespaces() {
    assertInitialized();
    return filterDatabases(new String[0]);
  }

  /**
   * List namespaces (databases) in a namespace.
   *
   * <p>MongoDB only supports top level namespaces (databases) (databases). So will return all
   * namespaces (databases) if namespace (database) is empty or any empty array if the namespace
   * (database) exists.
   *
   * @param namespace (database) a multi-part namespace
   * @return an array of multi-part namespace (database) names
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
   * Load metadata properties for a namespace.
   *
   * <p>Returns an empty map if the namespace (database) exists.
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
   * Create a namespace (database) in the catalog.
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
   * Apply a set of metadata changes to a namespace (database) in the catalog.
   *
   * <p>Throws {@code UnsupportedOperationException} as altering a namespace (database) is not
   * supported.
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

  /**
   * Drop a namespace (database) from the catalog.
   *
   * @param namespace (database) a multi-part namespace
   * @return true if the namespace (database) was dropped
   */
  @Override
  public boolean dropNamespace(final String[] namespace) {
    assertInitialized();
    if (namespaceExists(namespace)) {
      MongoConfig.writeConfig(options).doWithClient(c -> c.getDatabase(namespace[0]).drop());
      return true;
    }
    return false;
  }

  /**
   * List the tables (collections) in a namespace (database) from the catalog.
   *
   * <p>If the catalog supports views, this must return identifiers for only tables (collections)
   * and not views.
   *
   * @param namespace (database) a multi-part namespace
   * @return an array of Identifiers for tables (collections)
   */
  @Override
  public Identifier[] listTables(final String[] namespace) {
    return filterCollections(Identifier.of(namespace, ""));
  }

  /**
   * Test whether a table (collection) exists using an {@link Identifier identifier} from the
   * catalog.
   *
   * <p>If the catalog supports views and contains a view for the identifier and not a table, this
   * must return false.
   *
   * @param identifier a table (collection) identifier
   * @return true if the table (collection) exists, false otherwise
   */
  @Override
  public boolean tableExists(final Identifier identifier) {
    assertInitialized();
    if (identifier.namespace().length != 1) {
      return false;
    }
    return listTables(identifier.namespace()).length > 0;
  }

  /**
   * Load table (collection) metadata by {@link Identifier identifier} from the catalog.
   *
   * <p>If the catalog supports views and contains a view for the identifier and not a table, this
   * must throw {@link NoSuchTableException}.
   *
   * @param identifier a table (collection) identifier
   * @return the table's metadata
   * @throws NoSuchTableException If the table (collection) doesn't exist or is a view
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
    return new MongoTable(MongoConfig.readConfig(properties));
  }

  /**
   * Create a table (collection) in the catalog.
   *
   * @param identifier a table (collection) identifier
   * @param schema the schema of the new table, as a struct type
   * @param partitions transforms to use for partitioning data in the table
   * @param properties a string map of table (collection) properties
   * @return metadata for the new table
   * @throws TableAlreadyExistsException If a table (collection) or view already exists for the
   *     identifier
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
      throw new UnsupportedOperationException(
          format(
              "MongoCatalog.createTable does not support the following options: %s",
              String.join(",", properties.keySet())));
    }

    getWriteConfig()
        .doWithClient(
            c -> c.getDatabase(identifier.namespace()[0]).createCollection(identifier.name()));
    return new MongoTable(schema, getWriteConfig());
  }

  /**
   * Apply a set of {@link TableChange changes} to a table (collection) in the catalog.
   *
   * <p>Implementations may reject the requested changes. If any change is rejected, none of the
   * changes should be applied to the table.
   *
   * <p>The requested changes must be applied in the order given.
   *
   * <p>If the catalog supports views and contains a view for the identifier and not a table, this
   * must throw {@link NoSuchTableException}.
   *
   * @param identifier a table (collection) identifier
   * @param changes changes to apply to the table
   * @return updated metadata for the table
   * @throws NoSuchTableException If the table (collection) doesn't exist or is a view
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
   * Drop a table (collection) in the catalog.
   *
   * <p>If the catalog supports views and contains a view for the identifier and not a table, this
   * must not drop the view and must return false.
   *
   * @param identifier a table (collection) identifier
   * @return true if a table (collection) was deleted, false if no table (collection) exists for the
   *     identifier
   */
  @Override
  public boolean dropTable(final Identifier identifier) {
    assertInitialized();
    if (identifier.namespace().length == 1 && filterCollections(identifier).length != 0) {
      getWriteConfig()
          .doWithClient(
              c ->
                  c.getDatabase(identifier.namespace()[0]).getCollection(identifier.name()).drop());
      return true;
    }
    return false;
  }

  /**
   * Renames a table (collection) in the catalog.
   *
   * <p>If the catalog supports views and contains a view for the old identifier and not a table,
   * this throws {@link NoSuchTableException}. Additionally, if the new identifier is a table
   * (collection) or a view, this throws {@link TableAlreadyExistsException}.
   *
   * <p>If the catalog does not support table (collection) renames between namespaces, it throws
   * {@link UnsupportedOperationException}.
   *
   * @param oldIdentifier the table (collection) identifier of the existing table (collection) to
   *     rename
   * @param newIdentifier the new table (collection) identifier of the table
   * @throws NoSuchTableException If the table (collection) to rename doesn't exist or is a view
   * @throws TableAlreadyExistsException If the new table (collection) name already exists or is a
   *     view
   */
  @Override
  public void renameTable(final Identifier oldIdentifier, final Identifier newIdentifier)
      throws NoSuchTableException, TableAlreadyExistsException {
    if (!tableExists(oldIdentifier)) {
      throw new NoSuchTableException(oldIdentifier);
    } else if (tableExists(newIdentifier)) {
      throw new TableAlreadyExistsException(newIdentifier);
    }
    getWriteConfig()
        .doWithClient(
            c ->
                c.getDatabase(oldIdentifier.namespace()[0])
                    .getCollection(oldIdentifier.name())
                    .renameCollection(
                        new MongoNamespace(newIdentifier.namespace()[0], newIdentifier.name())));
  }

  private void assertInitialized() {
    Assertions.ensureState(() -> initialized, "The MongoCatalog is has not been initialized");
  }

  private String[][] filterDatabases(final String[] databaseName) {
    assertInitialized();
    if (databaseName.length > 1) {
      return new String[0][];
    }
    Bson filter =
        databaseName.length == 0
            ? NOT_SYSTEM_NAMESPACE
            : Filters.and(Filters.eq("name", databaseName[0]), NOT_SYSTEM_NAMESPACE);

    return getReadConfig()
        .withClient(
            client ->
                client
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
        () -> identifier.namespace().length == 1, "Namespace size must equal 1");

    Bson filter =
        identifier.name().isEmpty()
            ? IS_COLLECTION
            : Filters.and(Filters.eq("name", identifier.name()), IS_COLLECTION);

    return getReadConfig()
        .withClient(
            c ->
                c.getDatabase(identifier.namespace()[0])
                    .listCollections()
                    .filter(filter)
                    .map(d -> Identifier.of(identifier.namespace(), d.getString("name")))
                    .into(new ArrayList<>())
                    .toArray(new Identifier[0]));
  }

  @VisibleForTesting
  WriteConfig getWriteConfig() {
    assertInitialized();
    if (writeConfig == null) {
      writeConfig = MongoConfig.writeConfig(options);
    }
    return writeConfig;
  }

  @VisibleForTesting
  private ReadConfig getReadConfig() {
    assertInitialized();
    if (readConfig == null) {
      readConfig = MongoConfig.readConfig(options);
    }
    return readConfig;
  }

  @VisibleForTesting
  void reset(final Runnable onReset) {
    if (initialized) {
      onReset.run();
      initialized = false;
      name = null;
      options = null;
    }
  }
}
