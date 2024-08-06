/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import com.datasqrl.cmd.EngineKeys;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.IExecutionEngine;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.SimplePipeline;
import com.datasqrl.engine.stream.StreamEngine;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.google.inject.Injector;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Configuration for the engines
 * <p>
 */
public class PipelineFactory {

  private final Injector injector;
  private final List<String> enabledEngines;
  @NonNull
  @Getter
  private final PackageJson.EnginesConfig engineConfig;

  public PipelineFactory(Injector injector, List<String> enabledEngines, @NonNull PackageJson.EnginesConfig engineConfig) {
    this.injector = injector;
    this.enabledEngines = enabledEngines;
    this.engineConfig = engineConfig;
  }

  private Map<String, ExecutionEngine> getEngines(Optional<EngineFactory.Type> engineType) {
    Map<String, ExecutionEngine> engines = new HashMap<>();
    for (String engineId : enabledEngines) {
      if (engineId.equalsIgnoreCase(EngineKeys.TEST)) continue;
      EngineFactory engineFactory = ServiceLoaderDiscovery.get(
          EngineFactory.class,
          EngineFactory::getEngineName,
          engineId);

      IExecutionEngine engine = injector.getInstance(engineFactory.getFactoryClass());
      if (engineType.map(type -> engine.getType()==type).orElse(true)) {
        engines.put(engineId, (ExecutionEngine)engine);
      }
    }
    return engines;
  }

  public Map<String, ExecutionEngine> getEngines() {
    return getEngines(Optional.empty());
  }

  public Pair<String,ExecutionEngine> getEngine(EngineFactory.Type type) {
    Map<String,ExecutionEngine> engines = getEngines(Optional.of(type));
    //Todo: error collector
    ErrorCollector errors = ErrorCollector.root();
    errors.checkFatal(!engines.isEmpty(), "Need to configure a %s engine", type.name().toLowerCase());
    errors.checkFatal(engines.size()==1, "Currently support only a single %s engine", type.name().toLowerCase());
    return Pair.of(engines.entrySet().iterator().next());
  }

  public DatabaseEngine getDatabaseEngine() {
    return (DatabaseEngine) getEngine(EngineFactory.Type.DATABASE).getRight();
  }

  public StreamEngine getStreamEngine() {
    return (StreamEngine) getEngine(EngineFactory.Type.STREAMS).getRight();
  }


  public ExecutionPipeline createPipeline() {
    return SimplePipeline.of(getEngines(), /*todo*/ErrorCollector.root());
  }
}
