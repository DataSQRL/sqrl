package com.datasqrl.io.impl.inmem;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.DataSystemConnectorConfig;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.auto.service.AutoService;
import lombok.NonNull;

@AutoService(DataSystemConnectorConfig.class)
public class InMemConnector implements DataSystemConnectorConfig,
  DataSystemConnector {

  private final String name;
  private final TimeAnnotatedRecord[] data;

  public InMemConnector(String name, TimeAnnotatedRecord[] data) {
    this.name = name;
    this.data = data;
  }
  @Override
  public DataSystemConnector initialize(@NonNull ErrorCollector errors) {
    return this;
  }

  @Override
  @JsonIgnore
  public boolean hasSourceTimestamp() {
    return false;
  }

  @Override
  public String getSystemType() {
    return name;
  }

  public TimeAnnotatedRecord[] getData() {
    return data;
  }
}