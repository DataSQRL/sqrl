package com.datasqrl.engine.database.relational;

import com.datasqrl.calcite.convert.SnowflakeRelToSqlNode;
import com.datasqrl.calcite.convert.SnowflakeSqlNodeToString;
import com.datasqrl.calcite.dialect.ExtendedPostgresSqlDialect;
import com.datasqrl.calcite.dialect.snowflake.SqlCreateIcebergTableFromObjectStorage;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.database.relational.JdbcStatement.Type;
import com.datasqrl.engine.database.relational.ddl.statements.CreateTableDDL;
import com.datasqrl.plan.global.IndexDefinition;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SnowflakeStatementFactory extends AbstractJdbcStatementFactory {

  private final EngineConfig engineConfig;

  public SnowflakeStatementFactory(EngineConfig engineConfig) {
    super(new SnowflakeRelToSqlNode(), new SnowflakeSqlNodeToString()); //Iceberg does not support queries
    this.engineConfig = engineConfig;
  }

  @Override
  public JdbcStatement createTable(JdbcEngineCreateTable createTable) {
    String tableName = createTable.getTable().getTableName();
    return new JdbcStatement(tableName, Type.TABLE, getSnowflakeCreateTable(tableName));
  }

  public String getSnowflakeCreateTable(String tableName) {
    SqlLiteral externalVolume = SqlLiteral.createCharString(
        (String)engineConfig.toMap().get("external-volume"), SqlParserPos.ZERO);

    SqlCreateIcebergTableFromObjectStorage icebergTable = new SqlCreateIcebergTableFromObjectStorage(SqlParserPos.ZERO,
        true, false,
        new SqlIdentifier(tableName, SqlParserPos.ZERO),
        externalVolume,
        SqlLiteral.createCharString((String)engineConfig.toMap().get("catalog-name"),
            SqlParserPos.ZERO),
        SqlLiteral.createCharString(tableName, SqlParserPos.ZERO),
        null,
        null, null, null);

    return sqlNodeToString.convert(() -> icebergTable).getSql();
  }

  @Override
  protected SqlDataTypeSpec getSqlType(RelDataType type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public JdbcStatement addIndex(IndexDefinition indexDefinition) {
    throw new UnsupportedOperationException("Snowflake does not support indexes");
  }
}
