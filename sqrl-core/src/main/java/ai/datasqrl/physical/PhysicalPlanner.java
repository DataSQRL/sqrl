package ai.datasqrl.physical;

import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.config.util.StreamUtil;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.physical.database.MaterializedTableDDLBuilder;
import ai.datasqrl.physical.database.QueryBuilder;
import ai.datasqrl.physical.database.QueryTemplate;
import ai.datasqrl.physical.database.ddl.SqlDDLStatement;
import ai.datasqrl.physical.stream.StreamEngine;
import ai.datasqrl.physical.stream.flink.plan.FlinkPhysicalPlanner;
import ai.datasqrl.physical.stream.flink.plan.FlinkStreamPhysicalPlan;
import ai.datasqrl.plan.calcite.Planner;
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
  Planner planner;

  public PhysicalPlan plan(OptimizedDAG plan) {
    // 1. Create DDL for materialized tables
    List<OptimizedDAG.TableSink> materializedTables = StreamUtil.filterByClass(
            plan.getStreamQueries().stream().map(q -> q.getSink()), OptimizedDAG.TableSink.class)
            .collect(Collectors.toList());
    List<SqlDDLStatement> statements = new MaterializedTableDDLBuilder()
            .createTables(materializedTables, true);

    // 2. Plan Physical Stream Graph
    FlinkStreamPhysicalPlan streamPlan = new FlinkPhysicalPlanner(streamEngine, importManager,
            dbConnection)
        .createStreamGraph(plan.getStreamQueries());

    // 3. Create SQL queries
    QueryBuilder queryBuilder = new QueryBuilder(dbConnection.getDialect(),planner.getRelBuilder().getRexBuilder());
    Map<APIQuery, QueryTemplate> databaseQueries = queryBuilder.planQueries(plan.getDatabaseQueries());

    return new PhysicalPlan(dbConnection, statements, streamPlan, databaseQueries);
  }
}
