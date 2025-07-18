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

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.OperatorRuleTransformer;
import com.datasqrl.calcite.convert.PostgresRelToSqlNode;
import com.datasqrl.calcite.convert.PostgresSqlNodeToString;
import com.datasqrl.calcite.dialect.ExtendedPostgresSqlDialect;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan.Query;
import com.datasqrl.planner.hint.DataTypeHint;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class DuckDbStatementFactory extends AbstractJdbcStatementFactory {

  private final EngineConfig engineConfig;

  public DuckDbStatementFactory(EngineConfig engineConfig) {
    super(
        new OperatorRuleTransformer(Dialect.POSTGRES),
        new PostgresRelToSqlNode(),
        new PostgresSqlNodeToString()); // Iceberg does not support queries
    this.engineConfig = engineConfig;
  }

  @Override
  public JdbcDialect getDialect() {
    return JdbcDialect.Postgres;
  }

  @Override
  protected SqlDataTypeSpec getSqlType(RelDataType type, Optional<DataTypeHint> hint) {
    return ExtendedPostgresSqlDialect.DEFAULT.getCastSpec(type);
  }

  /**
   * DuckDB requires that we replace the tablescan with a function call that loads the iceberg
   * table. This is done at the RelNode level.
   *
   * @param query
   * @param withView
   * @param tableIdMap
   * @return
   */
  @Override
  public QueryResult createQuery(
      Query query, boolean withView, Map<String, JdbcEngineCreateTable> tableIdMap) {
    var relNode = query.getRelNode();
    var replaced =
        relNode.accept(
            new RelShuttleImpl() {
              @Override
              public RelNode visit(TableScan scan) {
                String tableId = scan.getTable().getQualifiedName().get(2);
                JdbcEngineCreateTable createTable = tableIdMap.get(tableId);
                Map<String, String> connector = createTable.getTable().getConnectorOptions();
                String warehouse = connector.get("warehouse");
                String databaseName =
                    connector.get("database-name") == null
                        ? "default_database"
                        : connector.get("database-name");
                RexBuilder rexBuilder = new RexBuilder(new TypeFactory());
                if (warehouse.startsWith("file://")) {
                  warehouse = warehouse.substring(7);
                }

                RexNode allowMovedPaths =
                    rexBuilder.makeCall(
                        SqlStdOperatorTable.EQUALS,
                        rexBuilder.makeFlag(Params.ALLOW_MOVED_PATHS),
                        rexBuilder.makeLiteral(true));
                RexNode rexNode =
                    rexBuilder.makeCall(
                        lightweightOp("iceberg_scan"),
                        rexBuilder.makeLiteral(
                            warehouse + "/" + databaseName + "/" + createTable.getTableName()),
                        allowMovedPaths);
                return new LogicalTableFunctionScan(
                    scan.getCluster(),
                    scan.getTraitSet(),
                    List.of(),
                    rexNode,
                    Object.class,
                    scan.getRowType(),
                    Set.of());
              }
            });

    return createQuery(
        query.getFunction().getSimpleName(), replaced, false, getTableNameMapping(tableIdMap));
  }

  @Override
  public JdbcStatement addIndex(IndexDefinition indexDefinition) {
    throw new UnsupportedOperationException("DuckDB does not support indexes");
  }

  enum Params {
    ALLOW_MOVED_PATHS
  }
}
