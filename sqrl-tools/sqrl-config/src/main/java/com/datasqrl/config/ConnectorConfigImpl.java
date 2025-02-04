package com.datasqrl.config;

import com.datasqrl.io.tables.TableType;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Getter
@Slf4j
public class ConnectorConfigImpl implements TableConfig.ConnectorConfig {

  protected final SqrlConfig config;
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

  public Optional<String> getFormat() {
    return config.asString(FORMAT_KEY).getOptional()
      .or(() -> config.asString(VALUE_FORMAT_KEY).getOptional());
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
    return config.asString(ConnectorConfigImpl.CONNECTOR_KEY).getOptional();
  }

  @Override
  public Map<String, Object> toMap() {
    return config.toMap();
  }

  @Override
  public void setProperty(String key, Object value) {
    config.setProperty(key, value);
  }

  @Override
  public String toString() {
    return "ConnectorConfigImpl{"+config.toMap().toString()+"}";
  }
}
