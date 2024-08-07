/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl;

import com.datasqrl.config.JdbcDialect;
import com.datasqrl.engine.database.relational.ddl.statements.CreateIndexDDL;
import com.datasqrl.engine.database.relational.ddl.statements.CreateTableDDL;
import com.datasqrl.sql.SqlDDLStatement;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.h2.api.H2Type;

@AutoService(JdbcDDLFactory.class)
public class H2DDLFactory implements JdbcDDLFactory {

  @Override
  public JdbcDialect getDialect() {
    return JdbcDialect.H2;
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
    }
    for (int pkIdx : table.getPrimaryKeys()) {
      RelDataTypeField field = fields.get(pkIdx);
      pk.add("\"" + field.getName() + "\"");
    }
    return new CreateTableDDL("\"" + table.getNameId() + "\"", columns, pk);
  }

  public static String toSql(RelDataTypeField field) {
    RelDataType datatype = field.getType();
    return toSql("\"" + field.getName() + "\"", getSQLType(datatype).getName(), datatype.isNullable());
  }

  private static String toSql(String name, String sqlType, boolean nullable) {
    StringBuilder sql = new StringBuilder();
    sql.append(name).append(" ").append(sqlType).append(" ");
    if (!nullable) {
      sql.append("NOT NULL");
    }
    return sql.toString();
  }

  private static H2Type getSQLType(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return H2Type.BOOLEAN;
      case TINYINT:
        return H2Type.TINYINT;
      case SMALLINT:
        return H2Type.SMALLINT;
      case BIGINT:
        return H2Type.BIGINT;
      case INTEGER:
        return H2Type.INTEGER;
      case CHAR:
        return H2Type.VARCHAR;
      case VARCHAR:
        return H2Type.VARCHAR;
      case FLOAT:
      case DOUBLE:
        return H2Type.DOUBLE_PRECISION;
      case DECIMAL:
        return H2Type.DECFLOAT;
      case DATE:
        return H2Type.DATE;
      case TIME:
        return H2Type.TIME;
      case TIMESTAMP:
        return H2Type.TIMESTAMP;
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return H2Type.TIMESTAMP_WITH_TIME_ZONE;
      case ARRAY:
        return H2Type.array(getSQLType(type.getComponentType()));
      case ROW:
        return H2Type.BLOB;
      case BINARY:
      case VARBINARY:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_DAY:
      case NULL:
      case SYMBOL:
      case MAP:
      case MULTISET:
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
