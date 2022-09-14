package ai.datasqrl.physical;

import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.config.util.StreamUtil;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.physical.database.MaterializedTableDDLBuilder;
import ai.datasqrl.physical.database.QueryTemplate;
import ai.datasqrl.physical.database.QueryBuilder;
import ai.datasqrl.physical.database.ddl.SqlDDLStatement;
import ai.datasqrl.physical.stream.StreamEngine;
import ai.datasqrl.physical.stream.flink.plan.FlinkStreamPhysicalPlan;
import ai.datasqrl.physical.stream.flink.plan.FlinkPhysicalPlanner;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.global.OptimizedDAG;
import ai.datasqrl.plan.queries.APIQuery;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
public class PhysicalPlanner {

  ImportManager importManager;
  JDBCConnectionProvider dbConnection;
  StreamEngine streamEngine;

  public PhysicalPlan plan(OptimizedDAG plan) {
    // 1. Create DDL for materialized tables
    List<VirtualRelationalTable> materializedTables = StreamUtil.filterByClass(
            plan.getStreamQueries().stream().map(q -> q.getSink()), OptimizedDAG.TableSink.class)
            .map(ts -> ts.getTable()).collect(Collectors.toList());
    List<SqlDDLStatement> statements = new MaterializedTableDDLBuilder()
            .createTables(materializedTables, true);

    // 2. Plan Physical Stream Graph
    FlinkStreamPhysicalPlan streamPlan = new FlinkPhysicalPlanner(streamEngine, importManager,
            dbConnection)
        .createStreamGraph(plan.getStreamQueries());

    // 3. Create SQL queries
    Map<APIQuery, QueryTemplate> databaseQueries = new QueryBuilder().planQueries(plan.getDatabaseQueries());

    return new PhysicalPlan(dbConnection, statements, streamPlan, databaseQueries);
  }
}
