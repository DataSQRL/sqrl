/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.OperatorRuleTransformer;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.convert.PostgresRelToSqlNode;
import com.datasqrl.calcite.convert.PostgresSqlNodeToString;
import com.datasqrl.calcite.dialect.ExtendedPostgresSqlDialect;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.engine.database.relational.AbstractJdbcStatementFactory;
import com.datasqrl.engine.database.relational.JdbcStatement;
import com.datasqrl.engine.database.relational.JdbcStatement.Field;
import com.datasqrl.engine.database.relational.JdbcStatement.Type;
import com.datasqrl.engine.database.relational.JdbcStatementFactory;
import com.datasqrl.engine.database.relational.ddl.statements.CreateIndexDDL;
import com.datasqrl.engine.database.relational.ddl.statements.CreateTableDDL;
import com.datasqrl.engine.database.relational.ddl.statements.InsertStatement;
import com.datasqrl.engine.database.relational.ddl.statements.notify.CreateNotifyTriggerDDL;
import com.datasqrl.engine.database.relational.ddl.statements.notify.ListenNotifyAssets;
import com.datasqrl.engine.database.relational.ddl.statements.notify.ListenQuery;
import com.datasqrl.engine.database.relational.ddl.statements.notify.OnNotifyQuery;
import com.datasqrl.engine.database.relational.ddl.statements.notify.Parameter;
import com.datasqrl.functions.vector.VectorPgExtension;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.sql.DatabaseExtension;
import com.datasqrl.v2.dag.plan.MaterializationStagePlan.Query;
import com.google.auto.service.AutoService;

@AutoService(JdbcDDLFactory.class)
public class PostgresDDLFactory extends AbstractJdbcStatementFactory implements JdbcDDLFactory, JdbcStatementFactory {

  public static final List<DatabaseExtension> EXTENSIONS = List.of(new VectorPgExtension());


  public PostgresDDLFactory() {
    super(new OperatorRuleTransformer(Dialect.POSTGRES), new PostgresRelToSqlNode(), new PostgresSqlNodeToString());
  }

  @Override
  public JdbcDialect getDialect() {
    return JdbcDialect.Postgres;
  }

  @Override
  protected SqlDataTypeSpec getSqlType(RelDataType type) {
    return ExtendedPostgresSqlDialect.DEFAULT.getCastSpec(type);
  }

  @Override
  public List<JdbcStatement> extractExtensions(List<Query> queries) {
    return extractTypeExtensions(queries.stream().map(Query::getRelNode), EXTENSIONS)
        .stream().map(ext -> new JdbcStatement(ext.getClass().getSimpleName(), Type.EXTENSION, ext.getExtensionDdl()))
        .collect(Collectors.toList());
  }

  @Override
  public JdbcStatement addIndex(IndexDefinition indexDefinition) {
    var ddl = createIndex(indexDefinition);
    return new JdbcStatement(ddl.getIndexName(), Type.INDEX, ddl.getSql());
  }

  /*
    Old methods
   */

  @Override
  @Deprecated
  public CreateIndexDDL createIndex(IndexDefinition index) {
    var columns = index.getColumnNames();
    return new CreateIndexDDL(index.getName(), index.getTableId(), columns, index.getType());
  }

  @Override
  @Deprecated
  public CreateTableDDL createTable(EngineSink table) {
    List<String> pk = new ArrayList<>();
    List<Field> columns = new ArrayList<>();

    var fields = table.getRowType().getFieldList();
    for (var i = 0; i < fields.size(); i++) {
      var field = fields.get(i);
      columns.add(toField(field));
    }
    for (int pkIdx : table.getPrimaryKeys()) {
      var field = fields.get(pkIdx);
      pk.add(quoteIdentifier(field.getName()));
    }
    return new CreateTableDDL(table.getNameId(), columns, pk);
  }

  /*
   The following methods are for the Postgres Log engine
   */

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


}
