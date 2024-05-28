/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl;

import com.datasqrl.calcite.dialect.ExtendedPostgresSqlDialect;
import com.datasqrl.calcite.dialect.ExtendedSnowflakeSqlDialect;
import com.datasqrl.calcite.dialect.snowflake.SqlColumnDeclaration;
import com.datasqrl.calcite.dialect.snowflake.SqlCreateIcebergTableFromAWSGlue;
import com.datasqrl.calcite.dialect.snowflake.SqlPrimaryKeyConstraint;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.engine.database.relational.ddl.statements.CreateIndexDDL;
import com.datasqrl.engine.database.relational.ddl.statements.CreateTableDDL;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.sql.SqlDDLStatement;
import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
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
    SqlIdentifier tableName = new SqlIdentifier(table.getNameId(), SqlParserPos.ZERO);

    return ()-> {
      SqlCreateIcebergTableFromAWSGlue createTable = new SqlCreateIcebergTableFromAWSGlue(
          SqlParserPos.ZERO, true,
          true, tableName, null, null,
          SqlLiteral.createCharString(table.getNameId(), SqlParserPos.ZERO), null,
          null, null);

      SqlWriterConfig config = SqlPrettyWriter.config()
          .withDialect(ExtendedSnowflakeSqlDialect.DEFAULT);
      SqlPrettyWriter prettyWriter = new SqlPrettyWriter(config);
      createTable.unparse(prettyWriter, 0, 0);
      return prettyWriter.toSqlString().getSql();
    };

  }

  public static String toSql(RelDataTypeField field) {
    SqlDataTypeSpec castSpec = ExtendedPostgresSqlDialect.DEFAULT.getCastSpec(field.getType());
    SqlPrettyWriter sqlPrettyWriter = new SqlPrettyWriter();
    castSpec.unparse(sqlPrettyWriter, 0, 0);
    String name = sqlPrettyWriter.toSqlString().getSql();

    RelDataType datatype = field.getType();


    return toSql(field.getName(), name, datatype.isNullable());
  }

  private static String toSql(String name, String sqlType, boolean nullable) {
    StringBuilder sql = new StringBuilder();
    sql.append("\"").append(name).append("\"").append(" ").append(sqlType).append(" ");
    if (!nullable) {
      sql.append("NOT NULL");
    }
    return sql.toString();
  }

  public CreateIndexDDL createIndex(IndexDefinition index) {
    List<String> columns = index.getColumnNames();
    return new CreateIndexDDL(index.getName(), index.getTableId(), columns, index.getType());
  }

  public static List<String> quoteIdentifier(List<String> columns) {
    return columns.stream()
        .map(SnowflakeDDLFactory::quoteIdentifier)
        .collect(Collectors.toList());
  }
  public static String quoteIdentifier(String column) {
    return "\"" + column + "\"";
  }
}
