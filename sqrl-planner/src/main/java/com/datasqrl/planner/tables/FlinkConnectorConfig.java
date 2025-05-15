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
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

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

  public static Map<String, TableType> CONNECTOR_TYPE_MAP =
      ImmutableMap.of(
          "kafka", TableType.STREAM,
          "file", TableType.STREAM,
          "filesystem", TableType.STREAM,
          "upsert-kafka", TableType.VERSIONED_STATE,
          "jdbc", TableType.LOOKUP,
          "jdbc-sqrl", TableType.LOOKUP,
          "postgres-cdc", TableType.VERSIONED_STATE);

  Map<String, String> options;

  @Override
  public Optional<String> getFormat() {
    return Optional.ofNullable(options.get(FORMAT_KEY))
        .or(() -> Optional.ofNullable(options.get(VALUE_FORMAT_KEY)));
  }

  @Override
  public TableType getTableType() {
    var connectorName = getConnectorName().get().toLowerCase();
    var tableType = CONNECTOR_TYPE_MAP.get(connectorName);
    if (tableType == null) {
      log.debug("Defaulting '{}' connector to STREAM table for import.", connectorName);
      tableType = TableType.STREAM;
    }
    return tableType;
  }

  @Override
  public Optional<String> getConnectorName() {
    return Optional.ofNullable(options.get(CONNECTOR_KEY));
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
