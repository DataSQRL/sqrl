/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.flink.connector.jdbc.dialect.h2;

import com.datasqrl.engine.database.relational.ddl.CreateIndexDDL;
import com.datasqrl.engine.database.relational.ddl.CreateTableDDL;
import com.datasqrl.engine.database.relational.ddl.SqlDDLStatement;
import com.datasqrl.engine.database.relational.dialect.JdbcDDLFactory;
import com.datasqrl.util.CalciteUtil;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.h2.api.H2Type;

public class H2DDLFactory implements JdbcDDLFactory {

  @Override
  public String getDialect() {
    return "h2";
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
        return H2Type.BOOLEAN.getName();
      case TINYINT:
        return H2Type.TINYINT.getName();
      case SMALLINT:
        return H2Type.SMALLINT.getName();
      case BIGINT:
        return H2Type.BIGINT.getName();
      case INTEGER:
        return H2Type.INTEGER.getName();
      case CHAR:
        return H2Type.VARCHAR.getName();
      case VARCHAR:
        return H2Type.VARCHAR.getName();
      case DECIMAL:
//        return H2Type.DOUBLE_PRECISION.getName();
      case FLOAT:
//        return H2Type.DOUBLE_PRECISION.getName();
      case DOUBLE:
        return "DECFLOAT(8)";
      case DATE:
        return H2Type.DATE.getName();
      case TIME:
        return H2Type.TIME.getName();
      case TIMESTAMP:
        return H2Type.TIMESTAMP.getName();
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return H2Type.TIMESTAMP_WITH_TIME_ZONE.getName();
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

  public CreateIndexDDL createIndex(IndexDefinition index) {
    List<String> columns = index.getColumnNames()
        .stream()
        .map(c->"\"" + c + "\"")
        .collect(Collectors.toList());
    return new CreateIndexDDL("\"" + index.getName() + "\"",
        "\"" + index.getTableId() + "\"", columns, index.getType());
  }
}
