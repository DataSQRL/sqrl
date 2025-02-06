package com.datasqrl.config;

import java.util.Map;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ConnectorConfImpl implements ConnectorConf {
  SqrlConfig sqrlConfig;

  @Override
  public Map<String, Object> toMap() {
    return sqrlConfig.toMap();
  }
}
