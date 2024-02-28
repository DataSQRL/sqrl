/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.SimplePipeline;
import com.datasqrl.engine.stream.StreamEngine;
import java.util.HashMap;
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

  public static final String ENGINES_PROPERTY = "engines";

  @NonNull
  @Getter //temporary to aid migration until DI is landed
  private final SqrlConfig config;

  public PipelineFactory(@NonNull SqrlConfig config) {
    this.config = config;
  }

  public static PipelineFactory fromRootConfig(@NonNull SqrlConfig config) {
    return new PipelineFactory(config.getSubConfig(ENGINES_PROPERTY));
  }

  private Map<String, ExecutionEngine> getEngines(Optional<ExecutionEngine.Type> engineType) {
    Map<String, ExecutionEngine> engines = new HashMap<>();
    for (String engineId : config.getKeys()) {
      SqrlConfig engineConfig = config.getSubConfig(engineId);
      EngineFactory engineFactory = EngineFactory.fromConfig(engineConfig);
      if (engineType.map(type -> engineFactory.getEngineType()==type).orElse(true)) {
        engines.put(engineId, engineFactory.initialize(engineConfig));
      }
    }
    return engines;
  }

  public Map<String, ExecutionEngine> getEngines() {
    return getEngines(Optional.empty());
  }

  public Pair<String,ExecutionEngine> getEngine(ExecutionEngine.Type type) {
    Map<String,ExecutionEngine> engines = getEngines(Optional.of(type));
    config.getErrorCollector().checkFatal(!engines.isEmpty(), "Need to configure a %s engine", type.name().toLowerCase());
    config.getErrorCollector().checkFatal(engines.size()==1, "Currently support only a single %s engine", type.name().toLowerCase());
    return Pair.of(engines.entrySet().iterator().next());
  }

  public DatabaseEngine getDatabaseEngine() {
    return (DatabaseEngine) getEngine(Type.DATABASE).getRight();
  }

  public StreamEngine getStreamEngine() {
    return (StreamEngine) getEngine(Type.STREAM).getRight();
  }


  public ExecutionPipeline createPipeline() {
    return SimplePipeline.of(getEngines(), config.getErrorCollector());
  }
}
