/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.config;

import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.QueryEngine;
import com.datasqrl.engine.export.PrintEngineFactory;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.SimplePipeline;
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
import lombok.Getter;
import lombok.NonNull;

/** Configuration for the engines */
public class PipelineFactory {

  public static final List<String> defaultEngines = List.of(PrintEngineFactory.NAME);
  public static final String TEST_ENGINE_NAME = "test";

  private final Injector injector;
  private final List<String> enabledEngines;
  @NonNull @Getter private final PackageJson.EnginesConfig engineConfig;

  public PipelineFactory(
      Injector injector,
      List<String> enabledEngines,
      @NonNull PackageJson.EnginesConfig engineConfig) {
    this.injector = injector;
    this.enabledEngines = enabledEngines;
    this.engineConfig = engineConfig;
  }

  public ExecutionPipeline createPipeline() {
    var errorCollector = injector.getInstance(ErrorCollector.class);
    return SimplePipeline.of(getEngines(), errorCollector);
  }

  public Map<String, ExecutionEngine> getEngines() {
    return getEngines(Optional.empty());
  }

  private Map<String, ExecutionEngine> getEngines(Optional<EngineType> engineType) {
    Map<String, ExecutionEngine> engines = new HashMap<>();
    Set<String> allEngines = new HashSet<>(enabledEngines);
    allEngines.addAll(defaultEngines);

    for (String engineId : allEngines) {
      if (engineId.equalsIgnoreCase(TEST_ENGINE_NAME)) {
        continue;
      }
      var engineFactory =
          ServiceLoaderDiscovery.get(EngineFactory.class, EngineFactory::getEngineName, engineId);

      var engine = injector.getInstance(engineFactory.getFactoryClass());
      if (engineType.map(type -> engine.getType() == type).orElse(true)) {
        engines.put(engineId, (ExecutionEngine) engine);
      }
    }
    // Register query engines with database engines that support them
    List<QueryEngine> queryEngines =
        StreamUtil.filterByClass(engines.values(), QueryEngine.class).toList();
    StreamUtil.filterByClass(engines.values(), DatabaseEngine.class)
        .forEach(
            databaseEngine ->
                queryEngines.stream()
                    .filter(databaseEngine::supportsQueryEngine)
                    .forEach(databaseEngine::addQueryEngine));
    return engines;
  }
}
