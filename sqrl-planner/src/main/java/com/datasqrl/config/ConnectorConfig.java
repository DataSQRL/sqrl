package com.datasqrl.config;

import com.datasqrl.io.tables.TableType;
import java.util.Map;
import java.util.Optional;

public interface ConnectorConfig {

  Map<String, Object> toMap();

  void setProperty(String key, Object value);

  //return optional<string>
  Optional<String> getFormat();

  TableType getTableType();

  Optional<String> getConnectorName();
}
