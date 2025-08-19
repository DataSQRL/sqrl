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

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.OperatorRuleTransformer;
import com.datasqrl.calcite.convert.SnowflakeRelToSqlNode;
import com.datasqrl.calcite.convert.SnowflakeSqlNodeToString;
import com.datasqrl.calcite.dialect.ExtendedPostgresSqlDialect;
import com.datasqrl.calcite.dialect.snowflake.SqlCreateIcebergTableFromObjectStorage;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.database.relational.JdbcStatement.Type;
import com.datasqrl.engine.database.relational.ddl.statements.GenericCreateTableDdlFactory;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.planner.hint.DataTypeHint;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SnowflakeStatementFactory extends AbstractJdbcStatementFactory {

  private final EngineConfig engineConfig;

  public SnowflakeStatementFactory(EngineConfig engineConfig) {
    super(
        new OperatorRuleTransformer(Dialect.SNOWFLAKE),
        new SnowflakeRelToSqlNode(),
        new SnowflakeSqlNodeToString(),
        new GenericCreateTableDdlFactory()); // Iceberg does not support queries
    this.engineConfig = engineConfig;
  }

  @Override
  public JdbcDialect getDialect() {
    return JdbcDialect.Snowflake;
  }

  @Override
  public JdbcStatement createTable(JdbcEngineCreateTable createTable) {
    var tableName = createTable.tableName();
    return new GenericJdbcStatement(tableName, Type.TABLE, getSnowflakeCreateTable(tableName));
  }

  public String getSnowflakeCreateTable(String tableName) {
    SqlLiteral externalVolume =
        SqlLiteral.createCharString(
            engineConfig.getSetting("external-volume", Optional.empty()), SqlParserPos.ZERO);

    var icebergTable =
        new SqlCreateIcebergTableFromObjectStorage(
            SqlParserPos.ZERO,
            true,
            false,
            new SqlIdentifier(tableName, SqlParserPos.ZERO),
            externalVolume,
            SqlLiteral.createCharString(
                engineConfig.getSetting("catalog-name", Optional.empty()), SqlParserPos.ZERO),
            SqlLiteral.createCharString(tableName, SqlParserPos.ZERO),
            null,
            null,
            null,
            null);

    return sqlNodeToString.convert(() -> icebergTable).getSql();
  }

  @Override
  protected SqlDataTypeSpec getSqlType(RelDataType type, Optional<DataTypeHint> hint) {
    // TODO: Need to create a snowflake spec or reuse Iceberg?
    return ExtendedPostgresSqlDialect.DEFAULT.getCastSpec(type);
  }

  @Override
  public JdbcStatement addIndex(IndexDefinition indexDefinition) {
    throw new UnsupportedOperationException("Snowflake does not support indexes");
  }
}
