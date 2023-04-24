/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.DatabaseEngineFactory;
import com.datasqrl.engine.pipeline.SimplePipeline;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.stream.StreamEngine;
import com.datasqrl.metadata.MetadataStoreProvider;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import lombok.NonNull;

import java.util.Optional;

/**
 * Configuration for the engines
 * <p>
 */
public class PipelineFactory {

  public static final String ENGINES_PROPERTY = "engines";

  @NonNull
  private final SqrlConfig config;

  public PipelineFactory(@NonNull SqrlConfig config) {
    this.config = config;
  }

  public static PipelineFactory fromRootConfig(@NonNull SqrlConfig config) {
    return new PipelineFactory(config.getSubConfig(ENGINES_PROPERTY));
  }


  public MetadataStoreProvider getMetaDataStoreProvider(Optional<String> engineIdentifier) {
    for (String engineId : config.getKeys()) {
      SqrlConfig engineConfig = config.getSubConfig(engineId);
      EngineFactory engineFactory = EngineFactory.fromConfig(engineConfig);
      if (engineIdentifier.map(id -> id.equalsIgnoreCase(engineId))
          .orElse(engineFactory.getEngineType()==Type.DATABASE)) {
        config.getErrorCollector().checkFatal(engineFactory instanceof DatabaseEngineFactory,
            "Selected or default engine [%s] for metadata is not a database engine", engineId);
        return ((DatabaseEngineFactory)engineFactory).getMetadataStore(engineConfig);
      }
    }
    throw config.getErrorCollector().exception("Could not find database engine for metadata");
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

  public ExecutionEngine getEngine(ExecutionEngine.Type type) {
    Collection<ExecutionEngine> engines = getEngines(Optional.of(type)).values();
    config.getErrorCollector().checkFatal(!engines.isEmpty(), "Need to configure a %s engine", type.name().toLowerCase());
    config.getErrorCollector().checkFatal(engines.size()==1, "Currently support only a single %s engine", type.name().toLowerCase());
    return engines.iterator().next();
  }

  public DatabaseEngine getDatabaseEngine() {
    return (DatabaseEngine) getEngine(Type.DATABASE);
  }

  public StreamEngine getStreamEngine() {
    return (StreamEngine) getEngine(Type.STREAM);
  }

  public ExecutionPipeline createPipeline() {
    DatabaseEngine db = getDatabaseEngine();
    StreamEngine stream = getStreamEngine();
    return SimplePipeline.of(stream, db);
  }



}
