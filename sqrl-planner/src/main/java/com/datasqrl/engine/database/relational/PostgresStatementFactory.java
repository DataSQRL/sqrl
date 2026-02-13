/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.engine.database.relational;

import static com.google.common.base.Preconditions.checkArgument;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.OperatorRuleTransformer;
import com.datasqrl.calcite.convert.PostgresRelToSqlNode;
import com.datasqrl.calcite.convert.PostgresSqlNodeToString;
import com.datasqrl.calcite.dialect.ExtendedPostgresSqlDialect;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.engine.database.relational.CreateTableJdbcStatement.PartitionType;
import com.datasqrl.engine.database.relational.JdbcStatement.Type;
import com.datasqrl.engine.database.relational.ddl.CreateIndexDDL;
import com.datasqrl.engine.database.relational.ddl.InsertStatement;
import com.datasqrl.engine.database.relational.ddl.PostgresCreateTableDdlFactory;
import com.datasqrl.engine.database.relational.ddl.notify.CreateNotifyTriggerDDL;
import com.datasqrl.function.translation.postgres.vector.VectorPgExtension;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan.Query;
import com.datasqrl.planner.hint.DataTypeHint;
import com.datasqrl.planner.hint.VectorDimensionHint;
import com.datasqrl.sql.DatabaseExtension;
import com.datasqrl.util.CalciteUtil;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAlienSystemTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.parser.SqlParserPos;

public class PostgresStatementFactory extends AbstractJdbcStatementFactory
    implements JdbcStatementFactory {

  public static final List<DatabaseExtension> EXTENSIONS = List.of(new VectorPgExtension());

  public PostgresStatementFactory() {
    super(
        new OperatorRuleTransformer(Dialect.POSTGRES),
        new PostgresRelToSqlNode(),
        new PostgresSqlNodeToString(),
        new PostgresCreateTableDdlFactory(true));
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
  protected PartitionType getPartitionType(
      JdbcEngineCreateTable createTable, List<String> partitionKey) {
    if (partitionKey.isEmpty()) {
      return PartitionType.NONE;
    }

    checkArgument(
        partitionKey.size() == 1,
        "Postgres only supports partitions for a single column on table %s. Combine these columns into one: %s",
        createTable.tableName(),
        partitionKey);

    String partitionCol = partitionKey.get(0);
    var colType = createTable.tableAnalysis().getRowType().getField(partitionCol, false, false);

    // Look up field reldatatype to determine partition type
    return CalciteUtil.isTimestamp(colType.getType()) ? PartitionType.RANGE : PartitionType.HASH;
  }

  @Override
  public List<JdbcStatement> extractExtensions(List<Query> queries) {
    return extractTypeExtensions(queries.stream().map(Query::relNode), EXTENSIONS).stream()
        .map(
            ext ->
                new GenericJdbcStatement(
                    ext.getClass().getSimpleName(), Type.EXTENSION, ext.getExtensionDdl()))
        .collect(Collectors.toList());
  }

  @Override
  public JdbcStatement addIndex(IndexDefinition index) {
    var ddl =
        new CreateIndexDDL(
            index.getName(), index.getTableName(), index.getColumnNames(), index.getType());
    return new GenericJdbcStatement(ddl.getIndexName(), Type.INDEX, ddl.getSql());
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
