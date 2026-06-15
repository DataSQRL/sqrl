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

import static com.datasqrl.flinkrunner.connector.postgresql.jdbc.SqrlPostgresOptions.SINK_ON_CONFLICT;
import static com.datasqrl.flinkrunner.connector.postgresql.jdbc.SqrlPostgresOptions.SINK_ON_CONFLICT_COLUMN;

import com.datasqrl.config.ConnectorConf;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson;
import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.datatype.flink.jdbc.FlinkSqrlPostgresDataTypeMapper;
import com.datasqrl.flinkrunner.connector.postgresql.jdbc.SqrlPostgresOptions.OnConflictAction;
import com.datasqrl.graphql.jdbc.DatabaseType;
import com.datasqrl.planner.analyzer.TableAnalysis;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.TreeMap;
import lombok.NonNull;
import org.apache.calcite.rel.type.RelDataTypeField;

public class PostgresJdbcEngine extends AbstractJDBCDatabaseEngine {

  @Inject
  public PostgresJdbcEngine(@NonNull PackageJson json, ConnectorFactoryFactory connectorFactory) {
    super(
        PostgresEngineFactory.ENGINE_NAME,
        json.getEngines().getEngineConfigOrEmpty(PostgresEngineFactory.ENGINE_NAME),
        connectorFactory);
  }

  @Override
  public DataTypeMapping getTypeMapping() {
    return new FlinkSqrlPostgresDataTypeMapper();
  }

  @Override
  protected JdbcDialect getDialect() {
    return JdbcDialect.Postgres;
  }

  @Override
  protected DatabaseType getDatabaseType() {
    return DatabaseType.POSTGRES;
  }

  @Override
  public JdbcStatementFactory getStatementFactory() {
    return new PostgresStatementFactory();
  }

  @Override
  protected Map<String, String> getConnectorOptions(
      ConnectorConf.Context.ContextBuilder ctxBuilder, TableAnalysis tableAnalysis) {

    var connectorOptions = super.getConnectorOptions(ctxBuilder, tableAnalysis);
    var mutableOptions = new TreeMap<>(connectorOptions);

    var tableType = tableAnalysis.getType();
    if (tableType.isStream()) {
      mutableOptions.putIfAbsent(SINK_ON_CONFLICT.key(), OnConflictAction.IGNORE.name());

      return mutableOptions;
    }

    if (tableType.isState()) {
      var tsCol =
          tableAnalysis.getRowTime().map(tableAnalysis::getField).map(RelDataTypeField::getName);

      if (tsCol.isPresent() && !mutableOptions.containsKey(SINK_ON_CONFLICT.key())) {
        mutableOptions.put(SINK_ON_CONFLICT.key(), OnConflictAction.TIMESTAMP.name());
        mutableOptions.put(SINK_ON_CONFLICT_COLUMN.key(), tsCol.get());

        return mutableOptions;
      }
    }

    // No PG-specific change, return as is
    return connectorOptions;
  }
}
