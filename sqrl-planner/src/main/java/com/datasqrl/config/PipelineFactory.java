/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import com.datasqrl.cmd.EngineKeys;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.IExecutionEngine;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.QueryEngine;
import com.datasqrl.engine.export.PrintEngine;
import com.datasqrl.engine.export.PrintEngineFactory;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.SimplePipeline;
import com.datasqrl.engine.stream.StreamEngine;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.datasqrl.util.StreamUtil;
import com.google.inject.Injector;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Configuration for the engines
 * <p>
 */
public class PipelineFactory {

  public static final List<String> defaultEngines = List.of(PrintEngineFactory.NAME);

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
    Set<String> allEngines = new HashSet<>(enabledEngines);
    allEngines.addAll(defaultEngines);
    for (String engineId : allEngines) {
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
    //Register query engines with database engines that support them
    List<QueryEngine> queryEngines = StreamUtil.filterByClass(engines.values(), QueryEngine.class).collect(
        Collectors.toUnmodifiableList());
    StreamUtil.filterByClass(engines.values(), DatabaseEngine.class).forEach(
        databaseEngine ->
          queryEngines.stream().filter(databaseEngine::supportsQueryEngine).forEach(databaseEngine::addQueryEngine)
    );
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

  public ExecutionPipeline createPipeline() {
    return SimplePipeline.of(getEngines(), /*todo*/ErrorCollector.root());
  }
}
