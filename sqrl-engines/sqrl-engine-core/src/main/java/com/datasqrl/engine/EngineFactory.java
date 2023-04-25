package com.datasqrl.engine;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Set;
import lombok.NonNull;

public interface EngineFactory {


  String getEngineName();

  ExecutionEngine.Type getEngineType();

  ExecutionEngine initialize(@NonNull SqrlConfig config);

  String ENGINE_NAME_KEY = "name";
  Set<String> RESERVED_KEYS = Set.of(ENGINE_NAME_KEY);
  static Set<String> getReservedKeys() {
    return RESERVED_KEYS;
  }
  static EngineFactory fromConfig(@NonNull SqrlConfig config) {
    return ServiceLoaderDiscovery.get(EngineFactory.class, EngineFactory::getEngineName,
        config.asString(ENGINE_NAME_KEY).get());
  }


}
