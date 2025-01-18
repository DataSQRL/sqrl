package com.datasqrl.flinkwrapper.tables;

import com.datasqrl.config.TableConfig;
import com.datasqrl.io.tables.TableType;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Getter
@Slf4j
public class FlinkConnectorConfigImpl implements TableConfig.ConnectorConfig {

  public static final String CONNECTOR_KEY = "connector";

  public static final String FORMAT_KEY = "format";
  public static final String VALUE_FORMAT_KEY = "value.format";

  public static Map<String,TableType> CONNECTOR_TYPE_MAP = ImmutableMap.of(
      "kafka",TableType.STREAM,
      "file", TableType.STREAM,
      "filesystem", TableType.STREAM,
      "upsert-kafka", TableType.VERSIONED_STATE,
      "jdbc", TableType.LOOKUP,
      "jdbc-sqrl", TableType.LOOKUP,
      "postgres-cdc", TableType.VERSIONED_STATE
  );


  Map<String, String> options;

  public Optional<String> getFormat() {
    return Optional.ofNullable(options.get(FORMAT_KEY))
        .or(() -> Optional.ofNullable(options.get(VALUE_FORMAT_KEY)));
  }

  @Override
  public TableType getTableType() {
    String connectorName = getConnectorName().get().toLowerCase();
    TableType tableType = CONNECTOR_TYPE_MAP.get(connectorName);
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
    return (Map)options;
  }

  @Override
  public void setProperty(String key, Object value) {
    options.put(key, String.valueOf(value));
  }

  @Override
  public String toString() {
    return "ConnectorConfigImpl{"+options.toString()+"}";
  }
}
