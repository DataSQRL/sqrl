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

import static com.datasqrl.config.SqrlConstants.FLINK_DEFAULT_DATABASE;
import static com.datasqrl.config.SqrlConstants.ICEBERG_CATALOG_DATABASE_KEY;
import static com.datasqrl.config.SqrlConstants.ICEBERG_CATALOG_IMPL_KEY;
import static com.datasqrl.config.SqrlConstants.ICEBERG_GLUE_CATALOG_IMPL;
import static com.datasqrl.config.SqrlConstants.ICEBERG_WAREHOUSE_KEY;
import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;
import static com.google.common.base.Preconditions.checkArgument;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.dialect.DuckDbSqlDialect;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.database.relational.DuckDbMaterializedScanCtePlanner.MaterializedScanCte;
import com.datasqrl.engine.database.relational.ddl.GenericCreateTableDdlFactory;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.planner.dag.plan.MaterializationStagePlan.Query;
import com.datasqrl.planner.hint.DataTypeHint;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class DuckDbStatementFactory extends AbstractJdbcStatementFactory {

  public static final String SCAN_CTE_CARDINALITY_DIVISOR = "scan-cte-cardinality-divisor";

  private final DuckDbMaterializedScanCtePlanner materializedScanCtePlanner;

  public DuckDbStatementFactory(EngineConfig engineConfig) {
    super(
        Dialect.DUCKDB,
        new GenericCreateTableDdlFactory(
            DuckDbSqlDialect.DEFAULT)); // Iceberg creates the tables, DuckDB only queries

    var scanCteCardinalityDivisor =
        Integer.parseInt(engineConfig.getSetting(SCAN_CTE_CARDINALITY_DIVISOR));
    checkArgument(
        Double.isFinite(scanCteCardinalityDivisor) && scanCteCardinalityDivisor > 0,
        "'%s' must be a positive finite number",
        SCAN_CTE_CARDINALITY_DIVISOR);
    materializedScanCtePlanner = new DuckDbMaterializedScanCtePlanner(scanCteCardinalityDivisor);
  }

  @Override
  protected SqlDataTypeSpec getSqlType(RelDataType type, Optional<DataTypeHint> hint) {
    return (SqlDataTypeSpec) DuckDbSqlDialect.DEFAULT.getCastSpec(type);
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

    var ctes = materializedScanCtePlanner.getMaterializedScanCtes(query.relNode());
    if (ctes.isEmpty()) {
      var replaced = query.relNode().accept(new IcebergTableScanRewriter(tableIdMap, Set.of()));
      return createQueryInternal(
          query.function().getSimpleName(),
          replaced,
          true,
          getTableNameMapping(tableIdMap),
          query.function().getDocumentation());
    }

    var tableNameMapping = new HashMap<>(getTableNameMapping(tableIdMap));
    ctes.forEach(cte -> tableNameMapping.put(cte.tableId(), cte.name()));

    var materializedTableIds =
        ctes.stream().map(MaterializedScanCte::tableId).collect(Collectors.toSet());
    var queryRelNode =
        query.relNode().accept(new IcebergTableScanRewriter(tableIdMap, materializedTableIds));
    var querySql = toSql(queryRelNode, tableNameMapping);
    var cteSql =
        ctes.stream()
            .map(
                cte ->
                    "%s AS MATERIALIZED (%s)"
                        .formatted(
                            cte.name(),
                            toSql(
                                cte.source()
                                    .accept(new IcebergTableScanRewriter(tableIdMap, Set.of())),
                                getTableNameMapping(tableIdMap))))
            .collect(Collectors.joining(", "));

    return createQueryInternal(
        query.function().getSimpleName(),
        query.relNode().getRowType(),
        true,
        "WITH %s %s".formatted(cteSql, querySql),
        query.function().getDocumentation());
  }

  private String toSql(RelNode relNode, Map<String, String> tableNameMapping) {
    var rewrittenRelNode = dialectCallConverter.convert(relNode);
    return sqlConverters.convert(sqlConverters.convert(rewrittenRelNode, tableNameMapping));
  }

  @Override
  public JdbcStatement addIndex(IndexDefinition indexDefinition) {
    throw new UnsupportedOperationException("DuckDB does not support indexes");
  }

  private static class IcebergTableScanRewriter extends RelShuttleImpl {

    private final Map<String, JdbcEngineCreateTable> tableIdMap;
    private final Set<String> materializedTableIds;
    private final RexShuttle subQueryRexShuttle =
        new RexShuttle() {
          @Override
          public RexNode visitSubQuery(RexSubQuery subQuery) {
            var rewritten = subQuery.rel.accept(IcebergTableScanRewriter.this);
            return subQuery.clone(rewritten);
          }
        };

    IcebergTableScanRewriter(
        Map<String, JdbcEngineCreateTable> tableIdMap, Set<String> materializedTableIds) {
      this.tableIdMap = tableIdMap;
      this.materializedTableIds = materializedTableIds;
    }

    @Override
    public RelNode visit(TableScan scan) {
      var tableId = DuckDbMaterializedScanCtePlanner.getTableId(scan);
      if (materializedTableIds.contains(tableId)) {
        return scan;
      }
      var createTable = tableIdMap.get(tableId);
      var connector = createTable.table().getConnectorOptions();

      var warehouse = connector.get(ICEBERG_WAREHOUSE_KEY);
      var databaseName =
          connector.getOrDefault(ICEBERG_CATALOG_DATABASE_KEY, FLINK_DEFAULT_DATABASE);
      if (ICEBERG_GLUE_CATALOG_IMPL.equals(connector.get(ICEBERG_CATALOG_IMPL_KEY))) {
        databaseName += ".db";
      }
      var rexBuilder = new RexBuilder(new TypeFactory());
      if (warehouse.startsWith("file://")) {
        warehouse = warehouse.substring(7);
      }

      var allowMovedPaths =
          rexBuilder.makeCall(
              SqlStdOperatorTable.EQUALS,
              rexBuilder.makeFlag(Params.ALLOW_MOVED_PATHS),
              rexBuilder.makeLiteral(true));
      var rexNode =
          rexBuilder.makeCall(
              lightweightOp("iceberg_scan"),
              rexBuilder.makeLiteral(
                  warehouse + "/" + databaseName + "/" + createTable.tableName()),
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

    @Override
    public RelNode visit(LogicalFilter filter) {
      var visited = (LogicalFilter) super.visit(filter);
      return visited.accept(subQueryRexShuttle);
    }

    @Override
    public RelNode visit(LogicalProject project) {
      var visited = (LogicalProject) super.visit(project);
      return visited.accept(subQueryRexShuttle);
    }

    enum Params {
      ALLOW_MOVED_PATHS
    }
  }
}
