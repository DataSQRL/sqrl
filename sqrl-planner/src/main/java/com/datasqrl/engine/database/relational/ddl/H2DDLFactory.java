/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.h2.api.H2Type;

import com.datasqrl.config.JdbcDialect;
import com.datasqrl.engine.database.relational.ddl.statements.CreateIndexDDL;
import com.datasqrl.engine.database.relational.ddl.statements.CreateTableDDL;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.sql.SqlDDLStatement;
import com.google.auto.service.AutoService;

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
    return toSql("\"" + field.getName() + "\"", getSQLType(datatype).getName(), datatype.isNullable());
  }

  private static String toSql(String name, String sqlType, boolean nullable) {
    var sql = new StringBuilder();
    sql.append(name).append(" ").append(sqlType).append(" ");
    if (!nullable) {
      sql.append("NOT NULL");
    }
    return sql.toString();
  }

  private static H2Type getSQLType(RelDataType type) {
    return switch (type.getSqlTypeName()) {
	case BOOLEAN -> H2Type.BOOLEAN;
	case TINYINT -> H2Type.TINYINT;
	case SMALLINT -> H2Type.SMALLINT;
	case BIGINT -> H2Type.BIGINT;
	case INTEGER -> H2Type.INTEGER;
	case CHAR -> H2Type.VARCHAR;
	case VARCHAR -> H2Type.VARCHAR;
	case FLOAT, DOUBLE -> H2Type.DOUBLE_PRECISION;
	case DECIMAL -> H2Type.DECFLOAT;
	case DATE -> H2Type.DATE;
	case TIME -> H2Type.TIME;
	case TIMESTAMP -> H2Type.TIMESTAMP;
	case TIMESTAMP_WITH_LOCAL_TIME_ZONE -> H2Type.TIMESTAMP_WITH_TIME_ZONE;
	case ARRAY -> H2Type.array(getSQLType(type.getComponentType()));
	case ROW -> H2Type.BLOB;
	case BINARY, VARBINARY, INTERVAL_YEAR_MONTH, INTERVAL_DAY, NULL, SYMBOL, MAP, MULTISET -> throw new UnsupportedOperationException("Unsupported type:" + type);
	default -> throw new UnsupportedOperationException("Unsupported type:" + type);
	};
  }

  @Override
public CreateIndexDDL createIndex(IndexDefinition index) {
    List<String> columns = index.getColumnNames()
        .stream()
        .map(c->"\"" + c + "\"")
        .collect(Collectors.toList());
    return new CreateIndexDDL("\"" + index.getName() + "\"",
        "\"" + index.getTableId() + "\"", columns, index.getType());
  }
}
