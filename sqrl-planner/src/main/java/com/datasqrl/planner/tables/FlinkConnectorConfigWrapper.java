/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.planner.tables;

import com.datasqrl.config.SqrlConstants;
import com.datasqrl.engine.database.relational.AbstractJDBCDatabaseEngine;
import com.datasqrl.engine.log.kafka.KafkaLogEngine;
import com.datasqrl.io.tables.TableType;
import com.google.common.collect.ImmutableMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;

/**
 * A wrapper around Flink table options for analysis and access to specific options that we need to
 * evaluate during planning.
 *
 * <p>This class is the one place for what the compiler knows about a connector by its name: {@link
 * #CONNECTORS}, {@link #CATALOG_CONNECTOR_MAP} and {@link #SINK_ONLY_CONNECTORS}. Add a connector,
 * or a new fact about connectors, here and not in a map of another class, so that the mappings
 * cannot drift apart.
 */
@Slf4j
public record FlinkConnectorConfigWrapper(Map<String, String> options, Optional<Catalog> catalog) {

  public static final String CONNECTOR_KEY = "connector";

  public static final String FORMAT_KEY = "format";
  public static final String VALUE_FORMAT_KEY = "value.format";
  public static final String KEY_FORMAT_KEY = "key.format";
  public static final String PATH_KEY = "path";

  /**
   * What the planner knows about the tables of a connector.
   *
   * @param tableType how the planner treats a table
   * @param storageType the kind of object that stores a table
   * @param storageNameKey the option that names that object
   */
  public record Connector(TableType tableType, StorageType storageType, String storageNameKey) {

    private static Connector topic(TableType tableType) {
      return new Connector(tableType, StorageType.TOPIC, KafkaLogEngine.CONNECTOR_TOPIC_KEY);
    }

    private static Connector file(TableType tableType) {
      return new Connector(tableType, StorageType.FILE, PATH_KEY);
    }

    private static Connector table(TableType tableType, String tableNameKey) {
      return new Connector(tableType, StorageType.TABLE, tableNameKey);
    }
  }

  public static final Map<String, Connector> CONNECTORS =
      ImmutableMap.<String, Connector>builder()
          .put("kafka", Connector.topic(TableType.STREAM))
          .put("kafka-safe", Connector.topic(TableType.STREAM))
          .put("file", Connector.file(TableType.STREAM))
          .put(
              "iceberg", Connector.table(TableType.STREAM, SqrlConstants.ICEBERG_CATALOG_TABLE_KEY))
          .put("filesystem", Connector.file(TableType.STREAM))
          .put("upsert-kafka", Connector.topic(TableType.VERSIONED_STATE))
          .put("upsert-kafka-safe", Connector.topic(TableType.VERSIONED_STATE))
          .put("jdbc", jdbcTable(TableType.LOOKUP))
          .put("jdbc-sqrl", jdbcTable(TableType.LOOKUP))
          .put("postgres-cdc", jdbcTable(TableType.VERSIONED_STATE))
          .put("mysql-cdc", jdbcTable(TableType.VERSIONED_STATE))
          .put("sqlserver-cdc", jdbcTable(TableType.VERSIONED_STATE))
          .build();

  private static Connector jdbcTable(TableType tableType) {
    return Connector.table(tableType, AbstractJDBCDatabaseEngine.CONNECTOR_TABLENAME_KEY);
  }

  public static Map<String, String> CATALOG_CONNECTOR_MAP =
      Map.of("org.apache.iceberg.flink.FlinkCatalog", "iceberg");

  public static final Set<String> SINK_ONLY_CONNECTORS =
      Set.of(
          "elasticsearch-6",
          "elasticsearch-7",
          "elasticsearch-8",
          "opensearch",
          "firehose",
          "dynamodb",
          "print",
          "blackhole",
          "prometheus");

  public Optional<String> getFormat() {
    return Optional.ofNullable(options.get(FORMAT_KEY))
        .or(() -> Optional.ofNullable(options.get(VALUE_FORMAT_KEY)));
  }

  public TableType getTableType() {
    var connectorName = getConnectorName();
    if (connectorName.isEmpty()) return TableType.RELATION;
    var connector = getConnector();
    if (connector.isEmpty()) {
      log.debug("Defaulting '{}' connector to STREAM table for import.", connectorName.get());
      return TableType.STREAM;
    }
    return connector.get().tableType();
  }

  /** What stores the table: a topic, a table or files. Empty if the connector is not known. */
  public Optional<StorageType> getStorageType() {
    return getConnector().map(Connector::storageType);
  }

  /** The name of what stores the table, e.g. the topic, if the options of its connector name it. */
  public Optional<String> getStorageName() {
    return getConnector().map(Connector::storageNameKey).map(options::get);
  }

  private Optional<Connector> getConnector() {
    return getConnectorName().map(name -> CONNECTORS.get(name.toLowerCase(Locale.ROOT)));
  }

  public boolean isSourceConnector() {
    return getConnectorName()
        .filter(name -> !SINK_ONLY_CONNECTORS.contains(name.toLowerCase()))
        .isPresent();
  }

  public Optional<String> getConnectorName() {
    var connector = options.get(CONNECTOR_KEY);
    if (connector == null && catalog.isPresent()) {
      if (!(catalog.get() instanceof GenericInMemoryCatalog)) {
        // it's an external catalog, let's see if we know it
        connector = CATALOG_CONNECTOR_MAP.get(catalog.get().getClass().getName());
        // otherwise we give it a name because we assume it's a source
        if (connector == null) connector = "unknown-catalog";
      }
    }
    return Optional.ofNullable(connector);
  }

  public void setProperty(String key, Object value) {
    options.put(key, String.valueOf(value));
  }

  @Override
  public String toString() {
    return "FlinkConnectorConfigWrapper{" + options.toString() + "}";
  }
}
