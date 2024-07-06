/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl;

import com.datasqrl.calcite.dialect.ExtendedSnowflakeSqlDialect;
import com.datasqrl.calcite.dialect.snowflake.SqlCreateIcebergTableFromObjectStorage;
import com.datasqrl.calcite.dialect.snowflake.SqlCreateIcebergTableFromSnowflake;
import com.datasqrl.calcite.dialect.snowflake.SqlCreateSnowflakeView;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.engine.database.relational.ddl.statements.CreateTableDDL;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.sql.SqlDDLStatement;
import com.google.auto.service.AutoService;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.experimental.Delegate;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

@AutoService(JdbcDDLFactory.class)
public class SnowflakeDDLFactory implements JdbcDDLFactory {

  @Override
  public JdbcDialect getDialect() {
    return JdbcDialect.Snowflake;
  }

  public SqlDDLStatement createTable(EngineSink table) {
    table.getRowType().getFieldList()
        .stream()
        .map(e->e.getName());
//    SqlCharStringLiteral comment = SqlLiteral.createCharString("This is an advanced view with multiple features.", pos);

    SqlLiteral externalVolume = null;
    SqlLiteral catalog = null;

    return new SnowflakeViewDelegate(new SqlCreateIcebergTableFromObjectStorage(SqlParserPos.ZERO,
        false, false,
        new SqlIdentifier(table.getNameId(), SqlParserPos.ZERO), externalVolume,
        catalog, null, null, null, null));
  }

  @Override
  public SqlDDLStatement createIndex(IndexDefinition indexDefinitions) {
    return null;
  }

  @AllArgsConstructor
  public class SnowflakeViewDelegate implements SqlDDLStatement {
    private SqlCreateIcebergTableFromObjectStorage view;

    @Override
    public String getSql() {
      SqlWriterConfig config = SqlPrettyWriter.config()
          .withDialect(ExtendedSnowflakeSqlDialect.DEFAULT)
          .withIndentation(0);
      SqlPrettyWriter prettyWriter = new SqlPrettyWriter(config);
      view.unparse(prettyWriter, 0, 0);
      return prettyWriter.toString();
    }
  }
}
