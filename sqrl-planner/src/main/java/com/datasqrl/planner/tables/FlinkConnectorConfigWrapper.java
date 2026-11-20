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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;

/**
 * A wrapper around Flink table options for analysis and access to specific options that we need to
 * evaluate during planning.
 */
@Slf4j
public record FlinkConnectorConfigWrapper(Map<String, String> options, Optional<Catalog> catalog) {

  public static final String CONNECTOR_KEY = "connector";

  public static final String FORMAT_KEY = "format";
  public static final String VALUE_FORMAT_KEY = "value.format";
  public static final String KEY_FORMAT_KEY = "key.format";
  public static final String PATH_KEY = "path";

  public static Map<String, TableType> CONNECTOR_TYPE_MAP =
      ImmutableMap.<String, TableType>builder()
          .put("kafka", TableType.STREAM)
          .put("kafka-safe", TableType.STREAM)
          .put("file", TableType.STREAM)
          .put("iceberg", TableType.STREAM)
          .put("filesystem", TableType.STREAM)
          .put("upsert-kafka", TableType.VERSIONED_STATE)
          .put("upsert-kafka-safe", TableType.VERSIONED_STATE)
          .put("jdbc", TableType.LOOKUP)
          .put("jdbc-sqrl", TableType.LOOKUP)
          .put("postgres-cdc", TableType.VERSIONED_STATE)
          .put("mysql-cdc", TableType.VERSIONED_STATE)
          .put("sqlserver-cdc", TableType.VERSIONED_STATE)
          .build();

  /** What stores the table of a connector, for the connectors of {@link #CONNECTOR_TYPE_MAP} */
  public static final Map<String, StorageType> CONNECTOR_STORAGE_MAP =
      ImmutableMap.<String, StorageType>builder()
          .put("kafka", StorageType.TOPIC)
          .put("kafka-safe", StorageType.TOPIC)
          .put("file", StorageType.FILE)
          .put("iceberg", StorageType.TABLE)
          .put("filesystem", StorageType.FILE)
          .put("upsert-kafka", StorageType.TOPIC)
          .put("upsert-kafka-safe", StorageType.TOPIC)
          .put("jdbc", StorageType.TABLE)
          .put("jdbc-sqrl", StorageType.TABLE)
          .put("postgres-cdc", StorageType.TABLE)
          .put("mysql-cdc", StorageType.TABLE)
          .put("sqlserver-cdc", StorageType.TABLE)
          .build();

  /** The options that name what stores the table, in the order they are looked up */
  private static final List<String> STORAGE_NAME_KEYS =
      List.of(
          KafkaLogEngine.CONNECTOR_TOPIC_KEY,
          AbstractJDBCDatabaseEngine.CONNECTOR_TABLENAME_KEY,
          SqrlConstants.ICEBERG_CATALOG_TABLE_KEY,
          PATH_KEY);

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
    var tableType = CONNECTOR_TYPE_MAP.get(connectorName.get().toLowerCase());
    if (tableType == null) {
      log.debug("Defaulting '{}' connector to STREAM table for import.", connectorName.get());
      tableType = TableType.STREAM;
    }
    return tableType;
  }

  /** What stores the table: a topic, a table or files. Empty if the connector is not known. */
  public Optional<StorageType> getStorageType() {
    return getConnectorName().map(name -> CONNECTOR_STORAGE_MAP.get(name.toLowerCase(Locale.ROOT)));
  }

  /** The name of what stores the table, e.g. the topic, if the options name it. */
  public Optional<String> getStorageName() {
    return STORAGE_NAME_KEYS.stream().map(options::get).filter(Objects::nonNull).findFirst();
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
