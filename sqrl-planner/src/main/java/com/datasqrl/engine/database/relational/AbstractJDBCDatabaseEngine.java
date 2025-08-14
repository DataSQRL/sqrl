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

import static com.datasqrl.engine.EngineFeature.STANDARD_DATABASE;

import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineType;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.database.QueryEngine;
import com.datasqrl.plan.global.IndexSelectorConfig;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/** Abstract implementation of a JDBC-compatible database engine. */
@Slf4j
public abstract class AbstractJDBCDatabaseEngine extends AbstractJDBCEngine
    implements DatabaseEngine {

  public static final String CONNECTOR_TABLENAME_KEY = "table-name";

  public AbstractJDBCDatabaseEngine(
      String name, @NonNull EngineConfig engineConfig, ConnectorFactoryFactory connectorFactory) {
    super(name, EngineType.DATABASE, STANDARD_DATABASE, engineConfig, connectorFactory);
  }

  @Override
  public boolean supportsQueryEngine(QueryEngine queryEngine) {
    return false;
  }

  @Override
  public void addQueryEngine(QueryEngine queryEngine) {
    throw new UnsupportedOperationException("JDBC database engines do not support query engines");
  }

  @Override
  protected String getConnectorTableName(FlinkTableBuilder tableBuilder) {
    return tableBuilder.getConnectorOptions().get(CONNECTOR_TABLENAME_KEY);
  }

  //  @Override
  //  public boolean supports(FunctionDefinition function) {
  //    //TODO: @Daniel: change to determining which functions are supported by dialect & database
  // type
  //    //This is a hack - we just check that it's not a tumble window function
  //    return FunctionUtil.getSqrlTimeTumbleFunction(function).isEmpty();
  //  }

  public IndexSelectorConfig getIndexSelectorConfig() {
    return IndexSelectorConfigByDialect.of(getDialect());
  }
}
