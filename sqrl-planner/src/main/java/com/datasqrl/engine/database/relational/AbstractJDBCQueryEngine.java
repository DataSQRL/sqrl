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

import static com.datasqrl.engine.EngineFeature.STANDARD_QUERY;

import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineType;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.database.QueryEngine;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import lombok.NonNull;

/** Abstract implementation of a relational {@link QueryEngine}. */
public abstract class AbstractJDBCQueryEngine extends AbstractJDBCEngine implements QueryEngine {

  public AbstractJDBCQueryEngine(
      String name, @NonNull EngineConfig engineConfig, ConnectorFactoryFactory connectorFactory) {
    super(name, EngineType.QUERY, STANDARD_QUERY, engineConfig, connectorFactory);
  }

  @Override
  protected String getConnectorTableName(FlinkTableBuilder tableBuilder) {
    throw new UnsupportedOperationException();
  }
}
