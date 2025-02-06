/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import com.datasqrl.config.JdbcDialect;
import com.datasqrl.engine.database.relational.ddl.statements.CreateTableDDL;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.sql.SqlDDLStatement;
import com.datasqrl.util.CalciteUtil;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;

@AutoService(JdbcDDLFactory.class)
public class SqliteDDLFactory implements JdbcDDLFactory {

  @Override
  public JdbcDialect getDialect() {
    return JdbcDialect.SQLite;
  }

  @Override
  public SqlDDLStatement createTable(EngineSink table) {
    List<String> pk = new ArrayList<>();
    List<String> columns = new ArrayList<>();

    var fields = table.getRowType().getFieldList();
    for (var i = 0; i < fields.size(); i++) {
      var field = fields.get(i);
      var column = toSql(field);
      columns.add(column);
    }
    for (int pkIdx : table.getPrimaryKeys()) {
      var field = fields.get(pkIdx);
      pk.add("\"" + field.getName() + "\"");
    }

    return new CreateTableDDL("\"" + table.getNameId() + "\"", columns, pk);
  }

  public static String toSql(RelDataTypeField field) {
    var datatype = field.getType();
    Preconditions.checkArgument(!CalciteUtil.isNestedTable(datatype),
        "Collection column encountered");
    return toSql("\"" + field.getName() + "\"", getSQLType(datatype), datatype.isNullable());
  }

  private static String toSql(String name, String sqlType, boolean nullable) {
    var sql = new StringBuilder();
    sql.append(name).append(" ").append(sqlType).append(" ");
    if (!nullable) {
      sql.append("NOT NULL");
    }
    return sql.toString();
  }

  private static String getSQLType(RelDataType type) {
    return switch (type.getSqlTypeName()) {
	case BOOLEAN -> "BOOLEAN";
	case TINYINT, SMALLINT, BIGINT, INTEGER -> "BIGINT";
	case CHAR, VARCHAR -> "VARCHAR";
	case DECIMAL -> "NUMERIC";
	case FLOAT, DOUBLE -> "DOUBLE";
	case DATE -> "DATE";
	case TIME -> "TIME";
	case TIMESTAMP -> "INTEGER";
	case TIMESTAMP_WITH_LOCAL_TIME_ZONE -> "INTEGER";
	case BINARY, VARBINARY, INTERVAL_YEAR_MONTH, INTERVAL_DAY, NULL, SYMBOL, ARRAY, MAP, MULTISET, ROW -> throw new UnsupportedOperationException("Unsupported type:" + type);
	default -> throw new UnsupportedOperationException("Unsupported type:" + type);
	};
  }

  @Override
  public SqlDDLStatement createIndex(IndexDefinition index) {
    List<String> columns = index.getColumnNames()
        .stream()
        .map(c->"\"" + c + "\"")
        .collect(Collectors.toList());
    return () -> String.format("CREATE INDEX IF NOT EXISTS %s ON %s (%s)",
        index.getName(), index.getTableId(),
        String.join(",", columns));
  }
}
