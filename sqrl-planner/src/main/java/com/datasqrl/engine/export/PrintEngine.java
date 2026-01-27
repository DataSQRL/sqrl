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
package com.datasqrl.engine.export;

import com.datasqrl.config.ConnectorConf;
import com.datasqrl.config.ConnectorConf.Context;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineType;
import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import jakarta.inject.Inject;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;

public class PrintEngine implements ExportEngine {

  private final ConnectorConf connectorConf;

  @Inject
  public PrintEngine(ConnectorFactoryFactory connectorFactory) {
    this.connectorConf = connectorFactory.getConfig(PrintEngineFactory.NAME);
  }

  @Override
  public EngineCreateTable createTable(
      ExecutionStage stage,
      String originalTableName,
      FlinkTableBuilder tableBuilder,
      RelDataType relDataType,
      Optional<TableAnalysis> tableAnalysis) {
    tableBuilder.setConnectorOptions(
        connectorConf.toMapWithSubstitution(
            Context.builder()
                .tableName(originalTableName)
                .tableId(tableBuilder.getTableName())
                .build()));
    return EngineCreateTable.NONE;
  }

  @Override
  public DataTypeMapping getTypeMapping() {
    return DataTypeMapping.NONE;
  }

  @Override
  public boolean supports(EngineFeature capability) {
    return false;
  }

  @Override
  public String getName() {
    return PrintEngineFactory.NAME;
  }

  @Override
  public EngineType getType() {
    return EngineType.EXPORT;
  }
}
