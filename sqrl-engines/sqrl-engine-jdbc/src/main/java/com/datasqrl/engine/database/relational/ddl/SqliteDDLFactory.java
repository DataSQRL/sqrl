/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl;

import com.datasqrl.engine.database.relational.ddl.statements.CreateTableDDL;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

@AutoService(JdbcDDLFactory.class)
public class SqliteDDLFactory implements JdbcDDLFactory {

  @Override
  public String getDialect() {
    return "sqlite";
  }

  @Override
  public SqlDDLStatement createTable(EngineSink table) {
    List<String> pk = new ArrayList<>();
    List<String> columns = new ArrayList<>();

    List<RelDataTypeField> fields = table.getRowType().getFieldList();
    for (int i = 0; i < fields.size(); i++) {
      RelDataTypeField field = fields.get(i);
      String column = toSql(field);
      columns.add(column);
      if (i < table.getNumPrimaryKeys()) {
        pk.add("\"" + field.getName() + "\"");
      }
    }

    return new CreateTableDDL("\"" + table.getNameId() + "\"", columns, pk);
  }

  public static String toSql(RelDataTypeField field) {
    RelDataType datatype = field.getType();
    Preconditions.checkArgument(!CalciteUtil.isNestedTable(datatype),
        "Collection column encountered");
    return toSql("\"" + field.getName() + "\"", getSQLType(datatype), datatype.isNullable());
  }

  private static String toSql(String name, String sqlType, boolean nullable) {
    StringBuilder sql = new StringBuilder();
    sql.append(name).append(" ").append(sqlType).append(" ");
    if (!nullable) {
      sql.append("NOT NULL");
    }
    return sql.toString();
  }

  private static String getSQLType(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return "BOOLEAN";
      case TINYINT:
      case SMALLINT:
      case BIGINT:
      case INTEGER:
        return "BIGINT";
      case CHAR:
      case VARCHAR:
        return "VARCHAR";
      case DECIMAL:
      case FLOAT:
      case DOUBLE:
        return "NUMERIC";
      case DATE:
        return "DATE";
      case TIME:
        return "TIME";
      case TIMESTAMP:
        return "INTEGER";
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return "INTEGER";
      case BINARY:
      case VARBINARY:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_DAY:
      case NULL:
      case SYMBOL:
      case ARRAY:
      case MAP:
      case MULTISET:
      case ROW:
      default:
        throw new UnsupportedOperationException("Unsupported type:" + type);
    }
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
