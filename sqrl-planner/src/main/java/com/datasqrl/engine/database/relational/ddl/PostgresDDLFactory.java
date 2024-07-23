/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl;

import com.datasqrl.calcite.dialect.ExtendedPostgresSqlDialect;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.engine.database.relational.ddl.statements.CreateIndexDDL;
import com.datasqrl.engine.database.relational.ddl.statements.CreateNotifyTriggerDDL;
import com.datasqrl.engine.database.relational.ddl.statements.CreateTableDDL;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.google.auto.service.AutoService;

import java.util.Collections;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

@AutoService(JdbcDDLFactory.class)
public class PostgresDDLFactory implements JdbcDDLFactory {

  @Override
  public JdbcDialect getDialect() {
    return JdbcDialect.Postgres;
  }

  @Override
  public CreateTableDDL createTable(EngineSink table) {
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
      pk.add(quoteIdentifier(field.getName()));
    }
    return new CreateTableDDL(table.getNameId(), columns, pk);
  }

  public CreateTableDDL createTable(String name, List<RelDataTypeField> fields, String primaryKey) {
    List<String> columns = fields.stream()
        .map(PostgresDDLFactory::toSql)
        .collect(Collectors.toList());

    return new CreateTableDDL(name, columns, Collections.singletonList(primaryKey));
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

  @Override
  public CreateIndexDDL createIndex(IndexDefinition index) {
    List<String> columns = index.getColumnNames();
    return new CreateIndexDDL(index.getName(), index.getTableId(), columns, index.getType());
  }

  public CreateNotifyTriggerDDL createNotify(String name, String pk) {
    return new CreateNotifyTriggerDDL(name, pk);
  }

  public static List<String> quoteIdentifier(List<String> columns) {
    return columns.stream()
        .map(PostgresDDLFactory::quoteIdentifier)
        .collect(Collectors.toList());
  }
  public static String quoteIdentifier(String column) {
    return "\"" + column + "\"";
  }
}
