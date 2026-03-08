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

import com.datasqrl.config.ConnectorConfig;
import com.datasqrl.io.tables.TableType;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;

/**
 * A wrapper around Flink table options for analysis and access to specific options that we need to
 * evaluate during planning.
 */
@AllArgsConstructor
@Getter
@Slf4j
public class FlinkConnectorConfig implements ConnectorConfig {

  public static final String CONNECTOR_KEY = "connector";

  public static final String FORMAT_KEY = "format";
  public static final String VALUE_FORMAT_KEY = "value.format";
  public static final String KEY_FORMAT_KEY = "key.format";

  public static Map<String, TableType> CONNECTOR_TYPE_MAP =
      ImmutableMap.of(
          "kafka", TableType.STREAM,
          "file", TableType.STREAM,
          "iceberg", TableType.STREAM,
          "filesystem", TableType.STREAM,
          "upsert-kafka", TableType.VERSIONED_STATE,
          "jdbc", TableType.LOOKUP,
          "jdbc-sqrl", TableType.LOOKUP,
          "postgres-cdc", TableType.VERSIONED_STATE);

  public static Map<String, String> CATALOG_CONNECTOR_MAP =
      ImmutableMap.of("org.apache.iceberg.flink.FlinkCatalog", "iceberg");

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

  Map<String, String> options;
  Optional<Catalog> catalog;

  @Override
  public Optional<String> getFormat() {
    return Optional.ofNullable(options.get(FORMAT_KEY))
        .or(() -> Optional.ofNullable(options.get(VALUE_FORMAT_KEY)));
  }

  @Override
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

  public boolean isSourceConnector() {
    return getConnectorName()
        .filter(name -> !SINK_ONLY_CONNECTORS.contains(name.toLowerCase()))
        .isPresent();
  }

  @Override
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

  @Override
  public boolean isEmpty() {
    return options.isEmpty();
  }

  @Override
  public Map<String, Object> toMap() {
    return (Map) options;
  }

  @Override
  public void setProperty(String key, Object value) {
    options.put(key, String.valueOf(value));
  }

  @Override
  public String toString() {
    return "ConnectorConfigImpl{" + options.toString() + "}";
  }


}
