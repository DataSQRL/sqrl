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
package com.datasqrl.engine.database.relational;

import static com.datasqrl.engine.EngineFeature.STANDARD_TABLE_FORMAT;

import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineType;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.database.AnalyticDatabaseEngine;
import com.datasqrl.engine.database.CombinedEnginePlan;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.QueryEngine;
import com.datasqrl.engine.export.ExportEngine;
import com.datasqrl.graphql.jdbc.DatabaseType;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan;
import com.google.common.base.Preconditions;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.NonNull;

/**
 * Abstract implementation of a relational table format database engine. A table format database
 * only persists data and does not provide an integrated query engine like implementations of {@link
 * AbstractJDBCDatabaseEngine} do.
 *
 * <p>Hence, a compatible {@link QueryEngine} must be registered with implementations of this class
 * for query execution.
 *
 * <p>The {@link com.datasqrl.engine.EnginePhysicalPlan} produced by a table format database has two
 * components: 1) The DDL statement for the Iceberg table that is created and the corresponding
 * catalog registration. 2) An {@link com.datasqrl.engine.EnginePhysicalPlan} for each registered
 * {@link QueryEngine} which contains a) the DDL for importing the Iceberg table from the catalog
 * and b) the queries translated to that engine.
 */
public abstract class AbstractJDBCTableFormatEngine extends AbstractJDBCEngine
    implements DatabaseEngine, AnalyticDatabaseEngine, ExportEngine {

  final ConnectorFactoryFactory connectorFactory;
  final Map<String, QueryEngine> queryEngines = new LinkedHashMap<>();

  public AbstractJDBCTableFormatEngine(
      String name, @NonNull EngineConfig engineConfig, ConnectorFactoryFactory connectorFactory) {
    super(name, EngineType.DATABASE, STANDARD_TABLE_FORMAT, engineConfig, connectorFactory);
    this.connectorFactory = connectorFactory;
  }

  @Override
  public void addQueryEngine(QueryEngine queryEngine) {
    if (!supportsQueryEngine(queryEngine)) {
      throw new UnsupportedOperationException(
          getName() + " table format does not support query engine: " + queryEngine);
    }
    Preconditions.checkState(
        !queryEngines.containsKey(queryEngine.getName()),
        "Query engine already added: %s",
        queryEngine.getName());
    queryEngines.put(queryEngine.getName(), queryEngine);
  }

  @Override
  protected DatabaseType getDatabaseType() {
    return DatabaseType.NONE;
  }

  @Override
  public boolean supports(EngineFeature capability) {
    return super.supports(capability)
        || queryEngines.values().stream().allMatch(queryEngine -> queryEngine.supports(capability));
  }

  @Override
  public EnginePhysicalPlan plan(MaterializationStagePlan stagePlan) {
    var planBuilder = CombinedEnginePlan.builder();
    planBuilder.plan("", super.plan(stagePlan));

    queryEngines.forEach(
        (name, engine) -> {
          planBuilder.plan(name, engine.plan(stagePlan));
        });

    return planBuilder.build();
  }
}
