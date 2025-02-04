package com.datasqrl.engine.database.relational;

import com.datasqrl.calcite.convert.PostgresSqlNodeToString;
import com.datasqrl.calcite.dialect.postgres.SqlCreatePostgresView;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.datatype.flink.jdbc.FlinkSqrlPostgresDataTypeMapper;
import com.datasqrl.engine.database.DatabaseEngine;
import com.google.inject.Inject;
import lombok.NonNull;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

public class PostgresJdbcEngine extends AbstractJDBCDatabaseEngine {

  PostgresSqlNodeToString sqlToString = new PostgresSqlNodeToString();

  @Inject
  public PostgresJdbcEngine(
      @NonNull PackageJson json,
      ConnectorFactoryFactory connectorFactory) {
    super(PostgresEngineFactory.ENGINE_NAME, json.getEngines().getEngineConfig(PostgresEngineFactory.ENGINE_NAME)
            .orElseGet(()-> new EmptyEngineConfig(PostgresEngineFactory.ENGINE_NAME)),
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
  protected String createView(SqlIdentifier viewNameIdentifier, SqlParserPos pos,
      SqlNodeList columnList, SqlNode viewSqlNode) {
    SqlCreatePostgresView createView = new SqlCreatePostgresView(pos, true,
        viewNameIdentifier, columnList,
        viewSqlNode);
    return sqlToString.convert(() -> createView).getSql() + ";";
  }
}
