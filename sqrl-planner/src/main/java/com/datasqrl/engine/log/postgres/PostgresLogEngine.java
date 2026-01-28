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
package com.datasqrl.engine.log.postgres;

import static com.datasqrl.config.EngineType.LOG;
import static com.datasqrl.engine.log.postgres.PostgresLogEngineFactory.ENGINE_NAME;

import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.datatype.flink.jdbc.FlinkSqrlPostgresDataTypeMapper;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.log.LogEngine;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import jakarta.inject.Inject;
import java.util.EnumSet;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;

public class PostgresLogEngine extends ExecutionEngine.Base implements LogEngine {

  @Getter private final EngineConfig engineConfig;

  @Inject
  public PostgresLogEngine(PackageJson json, ConnectorFactoryFactory connectorFactory) {
    super(ENGINE_NAME, LOG, EnumSet.noneOf(EngineFeature.class));

    this.engineConfig = json.getEngines().getEngineConfigOrEmpty(ENGINE_NAME);
  }

  @Override
  public EnginePhysicalPlan plan(MaterializationStagePlan stagePlan) {
    throw new UnsupportedOperationException("not yet supported");
  }

  @Override
  public EngineCreateTable createTable(
      ExecutionStage stage,
      String originalTableName,
      FlinkTableBuilder tableBuilder,
      RelDataType relDataType,
      Optional<TableAnalysis> tableAnalysis) {
    throw new UnsupportedOperationException("not yet supported");
  }

  @Override
  public DataTypeMapping getTypeMapping() {
    return new FlinkSqrlPostgresDataTypeMapper();
  }
}
