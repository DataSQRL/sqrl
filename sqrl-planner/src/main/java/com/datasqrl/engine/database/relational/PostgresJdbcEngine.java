package com.datasqrl.engine.database.relational;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

import com.datasqrl.calcite.convert.PostgresSqlNodeToString;
import com.datasqrl.calcite.dialect.postgres.SqlCreatePostgresView;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.datatype.DataTypeMapping;
import com.datasqrl.datatype.flink.jdbc.FlinkSqrlPostgresDataTypeMapper;
import com.datasqrl.engine.database.relational.ddl.PostgresDDLFactory;
import com.datasqrl.graphql.jdbc.DatabaseType;
import com.google.inject.Inject;

import lombok.NonNull;

public class PostgresJdbcEngine extends AbstractJDBCDatabaseEngine {

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
  protected DatabaseType getDatabaseType() {
    return DatabaseType.POSTGRES;
  }

  @Override
  public JdbcStatementFactory getStatementFactory() {
    return new PostgresDDLFactory();
  }

}
