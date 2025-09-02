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

import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson;
import com.datasqrl.graphql.jdbc.DatabaseType;
import com.google.inject.Inject;
import lombok.NonNull;

public class SnowflakeEngine extends AbstractJDBCQueryEngine {

  @Inject
  public SnowflakeEngine(@NonNull PackageJson json, ConnectorFactoryFactory connectorFactory) {
    super(
        SnowflakeEngineFactory.ENGINE_NAME,
        json.getEngines().getEngineConfigOrEmpty(SnowflakeEngineFactory.ENGINE_NAME),
        connectorFactory);
  }

  @Override
  public String serverConfigName() {
    return "snowflakeConfig";
  }

  @Override
  protected JdbcDialect getDialect() {
    return JdbcDialect.Snowflake;
  }

  @Override
  protected DatabaseType getDatabaseType() {
    return DatabaseType.SNOWFLAKE;
  }

  @Override
  public JdbcStatementFactory getStatementFactory() {
    return new SnowflakeStatementFactory(engineConfig);
  }
}
