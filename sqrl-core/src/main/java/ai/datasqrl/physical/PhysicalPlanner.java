package ai.datasqrl.physical;

import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.config.util.StreamUtil;
import ai.datasqrl.physical.database.relational.MaterializedTableDDLBuilder;
import ai.datasqrl.physical.database.relational.QueryBuilder;
import ai.datasqrl.physical.database.relational.QueryTemplate;
import ai.datasqrl.physical.database.relational.ddl.SqlDDLStatement;
import ai.datasqrl.physical.stream.StreamEngine;
import ai.datasqrl.physical.stream.flink.plan.FlinkPhysicalPlanner;
import ai.datasqrl.physical.stream.flink.plan.FlinkStreamPhysicalPlan;
import ai.datasqrl.plan.calcite.Planner;
import ai.datasqrl.plan.global.IndexCall;
import ai.datasqrl.plan.global.IndexDefinition;
import ai.datasqrl.plan.global.IndexSelector;
import ai.datasqrl.plan.global.OptimizedDAG;
import ai.datasqrl.plan.queries.APIQuery;
import ai.datasqrl.util.db.JDBCTempDatabase;
import lombok.AllArgsConstructor;

import java.util.*;
import java.util.stream.Collectors;

@AllArgsConstructor
public class PhysicalPlanner {

  JDBCConnectionProvider dbConnection;
  StreamEngine streamEngine;
  Planner planner;

  public PhysicalPlan plan(OptimizedDAG plan) {
    return plan(plan, Optional.empty());
  }

  public PhysicalPlan plan(OptimizedDAG plan, Optional<JDBCTempDatabase> jdbcTempDatabase) {
    // 1. Create DDL for materialized tables
    List<SqlDDLStatement> ddlStatements = new ArrayList<>();
    MaterializedTableDDLBuilder dbBuilder = new MaterializedTableDDLBuilder();
    List<OptimizedDAG.DatabaseSink> materializedTables = StreamUtil.filterByClass(
            plan.getStreamQueries().stream().map(q -> q.getSink()), OptimizedDAG.DatabaseSink.class)
            .collect(Collectors.toList());
    IndexSelector indexSelector = new IndexSelector(planner, IndexSelector.Config.of(dbConnection));
    ddlStatements.addAll(dbBuilder.createTables(materializedTables,true));
    Collection<IndexCall> indexCalls = plan.getDatabaseQueries().stream().map(indexSelector::getIndexSelection)
            .flatMap(List::stream).collect(Collectors.toList());
    Collection<IndexDefinition> indexDefinitions = indexSelector.optimizeIndexes(indexCalls).keySet();
    ddlStatements.addAll(dbBuilder.createIndexes(indexDefinitions, true));

    // 2. Plan Physical Stream Graph
    FlinkStreamPhysicalPlan streamPlan = new FlinkPhysicalPlanner(streamEngine, dbConnection, jdbcTempDatabase)
        .createStreamGraph(plan.getStreamQueries());

    // 3. Create SQL queries
    QueryBuilder queryBuilder = new QueryBuilder(dbConnection.getDialect(),planner.getRelBuilder().getRexBuilder());
    Map<APIQuery, QueryTemplate> databaseQueries = queryBuilder.planQueries(plan.getDatabaseQueries());

    return new PhysicalPlan(dbConnection, ddlStatements, streamPlan, databaseQueries);
  }
}
