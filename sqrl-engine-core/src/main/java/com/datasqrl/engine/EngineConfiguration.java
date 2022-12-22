/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.spi.JacksonDeserializer;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.NonNull;

import java.io.Serializable;

@JsonIgnoreProperties(value = "engineName", allowGetters = true)
public interface EngineConfiguration extends Serializable {

  String TYPE_KEY = "engineName";

  String getEngineName();

  @JsonIgnore
  ExecutionEngine.Type getEngineType();

  ExecutionEngine initialize(@NonNull ErrorCollector errors);

  class Deserializer extends JacksonDeserializer<EngineConfiguration> {

    public Deserializer() {
      super(EngineConfiguration.class, TYPE_KEY, EngineConfiguration::getEngineName);
    }
  }
}
