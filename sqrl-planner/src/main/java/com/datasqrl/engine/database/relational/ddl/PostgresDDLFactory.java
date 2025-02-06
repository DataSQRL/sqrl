/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.dialect.ExtendedPostgresSqlDialect;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.engine.database.relational.ddl.statements.CreateIndexDDL;
import com.datasqrl.engine.database.relational.ddl.statements.CreateTableDDL;
import com.datasqrl.engine.database.relational.ddl.statements.InsertStatement;
import com.datasqrl.engine.database.relational.ddl.statements.notify.CreateNotifyTriggerDDL;
import com.datasqrl.engine.database.relational.ddl.statements.notify.ListenNotifyAssets;
import com.datasqrl.engine.database.relational.ddl.statements.notify.ListenQuery;
import com.datasqrl.engine.database.relational.ddl.statements.notify.OnNotifyQuery;
import com.datasqrl.engine.database.relational.ddl.statements.notify.Parameter;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.google.auto.service.AutoService;

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

    var fields = table.getRowType().getFieldList();
    for (var i = 0; i < fields.size(); i++) {
      var field = fields.get(i);

      var column = toSql(field);
      columns.add(column);
    }
    for (int pkIdx : table.getPrimaryKeys()) {
      var field = fields.get(pkIdx);
      pk.add(quoteIdentifier(field.getName()));
    }
    return new CreateTableDDL(table.getNameId(), columns, pk);
  }

  public CreateTableDDL createTable(String name, List<RelDataTypeField> fields, List<String> primaryKeys) {
    var tableName = quoteIdentifier(name);

    List<String> columns = fields.stream()
        .map(PostgresDDLFactory::toSql)
        .collect(Collectors.toList());

    var pks = quoteValues(primaryKeys);

    return new CreateTableDDL(tableName, columns, pks);
  }

  public static String toSql(RelDataTypeField field) {
    var castSpec = ExtendedPostgresSqlDialect.DEFAULT.getCastSpec(field.getType());
    var sqlPrettyWriter = new SqlPrettyWriter();
    castSpec.unparse(sqlPrettyWriter, 0, 0);
    var name = sqlPrettyWriter.toSqlString().getSql();

    var datatype = field.getType();

    return toSql(field.getName(), name, datatype.isNullable());
  }

  private static String toSql(String name, String sqlType, boolean nullable) {
    var sql = new StringBuilder();
    sql.append("\"").append(name).append("\"").append(" ").append(sqlType).append(" ");
    if (!nullable) {
      sql.append("NOT NULL");
    }
    return sql.toString();
  }

  @Override
  public CreateIndexDDL createIndex(IndexDefinition index) {
    var columns = index.getColumnNames();
    return new CreateIndexDDL(index.getName(), index.getTableId(), columns, index.getType());
  }

  public CreateNotifyTriggerDDL createNotify(String name, List<String> primaryKeys) {
    return new CreateNotifyTriggerDDL(name, primaryKeys);
  }

  public ListenNotifyAssets createNotifyHelperDDLs(SqrlFramework framework, String tableName, RelDataType schema, List<String> primaryKeys) {
    var listenQuery = new ListenQuery(tableName);

    List<Parameter> parameters = primaryKeys.stream()
        .map(pk -> {
          var matcher = SqlNameMatchers.withCaseSensitive(false);
          RelDataTypeField matchedField = matcher.field(schema, pk);
          return new Parameter(pk, matchedField);
        })
        .collect(Collectors.toList());

    var onNotifyQuery = new OnNotifyQuery(framework, tableName, parameters);
    return new ListenNotifyAssets(listenQuery, onNotifyQuery, primaryKeys);
  }

  public InsertStatement createInsertHelperDMLs(String tableName, RelDataType tableSchema) {
    return new InsertStatement(tableName, tableSchema);
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
