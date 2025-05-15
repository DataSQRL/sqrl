/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.relational.ddl;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.OperatorRuleTransformer;
import com.datasqrl.calcite.convert.PostgresRelToSqlNode;
import com.datasqrl.calcite.convert.PostgresSqlNodeToString;
import com.datasqrl.calcite.dialect.ExtendedPostgresSqlDialect;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.engine.database.relational.AbstractJdbcStatementFactory;
import com.datasqrl.engine.database.relational.JdbcStatement;
import com.datasqrl.engine.database.relational.JdbcStatement.Type;
import com.datasqrl.engine.database.relational.JdbcStatementFactory;
import com.datasqrl.engine.database.relational.ddl.statements.CreateIndexDDL;
import com.datasqrl.engine.database.relational.ddl.statements.InsertStatement;
import com.datasqrl.engine.database.relational.ddl.statements.notify.CreateNotifyTriggerDDL;
import com.datasqrl.function.vector.VectorPgExtension;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.sql.DatabaseExtension;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan.Query;
import com.datasqrl.planner.hint.DataTypeHint;
import com.datasqrl.planner.hint.VectorDimensionHint;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.parser.SqlParserPos;

public class PostgresDDLFactory extends AbstractJdbcStatementFactory
    implements JdbcStatementFactory {

  public static final List<DatabaseExtension> EXTENSIONS = List.of(new VectorPgExtension());

  public PostgresDDLFactory() {
    super(
        new OperatorRuleTransformer(Dialect.POSTGRES),
        new PostgresRelToSqlNode(),
        new PostgresSqlNodeToString());
  }

  @Override
  public JdbcDialect getDialect() {
    return JdbcDialect.Postgres;
  }

  @Override
  protected SqlDataTypeSpec getSqlType(RelDataType type, Optional<DataTypeHint> hint) {
    SqlDataTypeSpec spec = ExtendedPostgresSqlDialect.DEFAULT.getCastSpec(type);
    Optional<VectorDimensionHint> vecDimOpt =
        hint.filter(VectorDimensionHint.class::isInstance).map(VectorDimensionHint.class::cast);
    if (vecDimOpt.isPresent()) {
      spec =
          new SqlDataTypeSpec(
              new SqlAlienSystemTypeNameSpec(
                  "VECTOR(" + vecDimOpt.get().getDimensions() + ")",
                  type.getSqlTypeName(),
                  SqlParserPos.ZERO),
              SqlParserPos.ZERO);
    }
    return spec;
  }

  @Override
  public List<JdbcStatement> extractExtensions(List<Query> queries) {
    return extractTypeExtensions(queries.stream().map(Query::getRelNode), EXTENSIONS).stream()
        .map(
            ext ->
                new JdbcStatement(
                    ext.getClass().getSimpleName(), Type.EXTENSION, ext.getExtensionDdl()))
        .collect(Collectors.toList());
  }

  @Override
  public JdbcStatement addIndex(IndexDefinition index) {
    var ddl = new CreateIndexDDL(index.getName(), index.getTableId(), index.getColumnNames(), index.getType());
    return new JdbcStatement(ddl.getIndexName(), Type.INDEX, ddl.getSql());
  }

  /*
  The following methods are for the Postgres Log engine - we'll keep those around for now
  */

  public CreateNotifyTriggerDDL createNotify(String name, List<String> primaryKeys) {
    return new CreateNotifyTriggerDDL(name, primaryKeys);
  }

  public InsertStatement createInsertHelperDMLs(String tableName, RelDataType tableSchema) {
    return new InsertStatement(tableName, tableSchema);
  }
}
