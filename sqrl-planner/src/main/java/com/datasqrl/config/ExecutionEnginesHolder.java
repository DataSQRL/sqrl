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
import com.datasqrl.engine.database.relational.DuckDBEngineFactory;
import com.datasqrl.engine.database.relational.IcebergEngineFactory;
import com.datasqrl.engine.database.relational.PostgresEngineFactory;
import com.datasqrl.engine.database.relational.SnowflakeEngineFactory;
import com.datasqrl.engine.export.PrintEngineFactory;
import com.datasqrl.engine.log.kafka.KafkaLogEngine;
import com.datasqrl.engine.server.VertxEngineFactory;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.datasqrl.util.StreamUtil;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import lombok.RequiredArgsConstructor;

/** Configuration for the engines */
@RequiredArgsConstructor
@Singleton
public class ExecutionEnginesHolder {

  private static final List<String> defaultEngines = List.of(PrintEngineFactory.NAME);

  private static final String DUCKDB_NAME = DuckDBEngineFactory.ENGINE_NAME;
  private static final String POSTGRES_NAME = PostgresEngineFactory.ENGINE_NAME;
  private static final String SNOWFLAKE_NAME = SnowflakeEngineFactory.ENGINE_NAME;
  private static final String VERTX_NAME = VertxEngineFactory.ENGINE_NAME;

  private final ErrorCollector errors;
  private final Injector injector;
  private final PackageJson packageJson;
  private final boolean testExecution;

  private volatile Map<String, ExecutionEngine> engines = null;

  public void initEnabledEngines() {
    getEngines();

    if (testExecution) {
      packageJson.setEnabledEngines(List.copyOf(engines.keySet()));
    }
  }

  public Map<String, ExecutionEngine> getEngines() {
    return getEngines(null);
  }

  public Map<String, ExecutionEngine> getEngines(
      @Nullable Class<? extends ExecutionEngine> filterClass) {

    if (engines == null) {
      synchronized (this) {
        if (engines == null) {
          Map<String, ExecutionEngine> enginesTmp = new TreeMap<>();

          Set<String> allEngines = new HashSet<>(packageJson.getEnabledEngines());
          allEngines.addAll(defaultEngines);

          for (String engineName : allEngines) {
            enginesTmp.put(engineName, getEngineInstance(engineName));
          }

          // Register query engines with database engines that support them
          List<QueryEngine> queryEngines =
              StreamUtil.filterByClass(enginesTmp.values(), QueryEngine.class).toList();

          StreamUtil.filterByClass(enginesTmp.values(), DatabaseEngine.class)
              .forEach(
                  databaseEngine ->
                      queryEngines.stream()
                          .filter(databaseEngine::supportsQueryEngine)
                          .forEach(databaseEngine::addQueryEngine));

          if (testExecution) {
            enginesTmp = adaptToTest(enginesTmp);
          }

          engines = Collections.unmodifiableMap(enginesTmp);
        }
      }
    }

    if (filterClass == null) {
      return engines;
    }

    var filteredEngines = new TreeMap<String, ExecutionEngine>();
    StreamUtil.filterByClass(engines.values(), filterClass)
        .forEach(e -> filteredEngines.put(e.getName(), e));

    return Collections.unmodifiableMap(filteredEngines);
  }

  ExecutionEngine getEngineInstance(String engineName) {
    var engineFactory =
        ServiceLoaderDiscovery.get(EngineFactory.class, EngineFactory::getEngineName, engineName);

    return (ExecutionEngine) injector.getInstance(engineFactory.getFactoryClass());
  }

  Map<String, ExecutionEngine> adaptToTest(Map<String, ExecutionEngine> engines) {
    if (engines.containsKey(SNOWFLAKE_NAME)) {
      errors.fatal(
          "'%s' as a query engine is not supported for 'test' executions. Please use '%s' instead.",
          SNOWFLAKE_NAME, DUCKDB_NAME);
    }

    // Add server if missing
    if (engines.values().stream().noneMatch(e -> e.getType() == EngineType.SERVER)) {
      engines.put(VERTX_NAME, getEngineInstance(VERTX_NAME));
    }

    // Add query engine for iceberg if missing
    if (engines.containsKey(IcebergEngineFactory.ENGINE_NAME)
        && !engines.containsKey(DUCKDB_NAME)) {
      engines.put(DUCKDB_NAME, getEngineInstance(DUCKDB_NAME));
    }

    // Add postgres if no real DB engine
    var realDbEngines =
        engines.values().stream()
            .filter(e -> e.getType() == EngineType.DATABASE)
            .filter(e -> !(e instanceof KafkaLogEngine))
            .count();

    if (realDbEngines == 0) {
      engines.put(POSTGRES_NAME, getEngineInstance(POSTGRES_NAME));
    }

    return engines;
  }
}
