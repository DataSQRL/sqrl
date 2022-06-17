package ai.datasqrl.physical;

import ai.datasqrl.config.engines.JDBCConfiguration;
import ai.datasqrl.physical.stream.StreamEngine;
import ai.datasqrl.physical.database.MaterializedTableDDLBuilder;
import ai.datasqrl.physical.database.ViewDDLBuilder;
import ai.datasqrl.physical.database.ddl.SqlDDLStatement;
import ai.datasqrl.physical.stream.flink.plan.CreateStreamJobResult;
import ai.datasqrl.physical.stream.flink.plan.StreamGraphBuilder;
import ai.datasqrl.plan.LogicalPlan;
import ai.datasqrl.environment.ImportManager;
import java.util.List;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class PhysicalPlanner {

  ImportManager importManager;
  JDBCConfiguration jdbcConfiguration;
  StreamEngine streamEngine;

  public PhysicalPlan plan(LogicalPlan plan) {
    CreateStreamJobResult result = new StreamGraphBuilder(streamEngine, importManager,
        jdbcConfiguration)
        .createStreamGraph(plan.getStreamQueries());

    List<SqlDDLStatement> statements = new MaterializedTableDDLBuilder()
        .create(result.getCreatedTables(), true);
    statements.addAll(new ViewDDLBuilder()
        .create(plan.getDatabaseQueries()));

    return new PhysicalPlan(statements, result.getStreamQueries(), plan.getSchema());
  }
}
