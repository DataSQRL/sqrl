/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.dialect.ExtendedPostgresSqlDialect;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.engine.database.relational.ddl.statements.CreateIndexDDL;
import com.datasqrl.engine.database.relational.ddl.statements.notify.ListenNotifyAssets;
import com.datasqrl.engine.database.relational.ddl.statements.notify.OnNotifyQuery;
import com.datasqrl.engine.database.relational.ddl.statements.notify.ListenQuery;
import com.datasqrl.engine.database.relational.ddl.statements.notify.CreateNotifyTriggerDDL;
import com.datasqrl.engine.database.relational.ddl.statements.CreateTableDDL;
import com.datasqrl.engine.database.relational.ddl.statements.notify.Parameter;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.google.auto.service.AutoService;

import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlNameMatchers;

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

  public CreateTableDDL createTable(String name, List<RelDataTypeField> fields, List<String> primaryKeys) {
    String tableName = quoteIdentifier(name);

    List<String> columns = fields.stream()
        .map(PostgresDDLFactory::toSql)
        .collect(Collectors.toList());

    List<String> pks = quoteValues(primaryKeys);

    return new CreateTableDDL(tableName, columns, pks);
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

  public CreateNotifyTriggerDDL createNotify(String name, List<String> primaryKeys) {
    return new CreateNotifyTriggerDDL(name, primaryKeys);
  }

  public ListenNotifyAssets createNotifyHelperDDLs(SqrlFramework framework, String name, RelDataType schema, List<String> primaryKeys) {
    ListenQuery listenQuery = new ListenQuery(name);

    List<Parameter> parameters = primaryKeys.stream()
        .map(pk -> {
          SqlNameMatcher matcher = SqlNameMatchers.withCaseSensitive(false);
          RelDataTypeField matchedField = matcher.field(schema, pk);
          return new Parameter(pk, matchedField);
        })
        .collect(Collectors.toList());

    OnNotifyQuery onNotifyQuery = new OnNotifyQuery(framework, name, parameters);
    return new ListenNotifyAssets(listenQuery, onNotifyQuery, primaryKeys);
  }

  public static List<String> quoteIdentifier(List<String> columns) {
    return columns.stream()
        .map(PostgresDDLFactory::quoteIdentifier)
        .collect(Collectors.toList());
  }
  public static String quoteIdentifier(String column) {
    return "\"" + column + "\"";
  }

  public static List<String> quoteValues(List<String> values) {
    return values.stream()
        .map(PostgresDDLFactory::quoteIdentifier)
        .collect(Collectors.toList());
  }
}
