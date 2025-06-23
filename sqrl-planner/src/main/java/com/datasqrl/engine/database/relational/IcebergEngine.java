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
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.datatype.flink.iceberg.IcebergDataTypeMapper;
import com.datasqrl.engine.database.QueryEngine;
import com.datasqrl.planner.tables.FlinkTableBuilder;
import com.google.inject.Inject;
import lombok.NonNull;

public class IcebergEngine extends AbstractJDBCTableFormatEngine {

  public static final String CONNECTOR_TABLENAME_KEY = "catalog-table";

  @Inject
  public IcebergEngine(@NonNull PackageJson json, ConnectorFactoryFactory connectorFactory) {
    super(
        IcebergEngineFactory.ENGINE_NAME,
        json.getEngines()
            .getEngineConfig(IcebergEngineFactory.ENGINE_NAME)
            .orElseGet(() -> new EmptyEngineConfig(IcebergEngineFactory.ENGINE_NAME)),
        connectorFactory);
  }

  @Override
  public boolean supportsQueryEngine(QueryEngine engine) {
    return engine instanceof SnowflakeEngine || engine instanceof DuckDBEngine;
  }

  @Override
  protected JdbcDialect getDialect() {
    return JdbcDialect.Iceberg;
  }

  @Override
  public String getConnectorTableName(FlinkTableBuilder tableBuilder) {
    return tableBuilder.getConnectorOptions().get(CONNECTOR_TABLENAME_KEY);
  }

  @Override
  public JdbcStatementFactory getStatementFactory() {
    return new IcebergStatementFactory();
  }

  @Override
  public DataTypeMapping getTypeMapping() {
    return new IcebergDataTypeMapper();
  }
}
