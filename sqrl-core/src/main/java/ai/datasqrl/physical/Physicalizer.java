package ai.datasqrl.physical;

import ai.datasqrl.config.engines.JDBCConfiguration;
import ai.datasqrl.execute.StreamEngine;
import ai.datasqrl.physical.database.StreamTableDDLBuilder;
import ai.datasqrl.physical.database.ViewDDLBuilder;
import ai.datasqrl.physical.database.ddl.SqlDDLStatement;
import ai.datasqrl.physical.stream.CreateStreamJobResult;
import ai.datasqrl.physical.stream.StreamGraphBuilder;
import ai.datasqrl.plan.LogicalPlan;
import ai.datasqrl.server.ImportManager;
import java.util.List;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class Physicalizer {
  ImportManager importManager;
  JDBCConfiguration jdbcConfiguration;
  StreamEngine streamEngine;

  public ExecutionPlan plan(LogicalPlan plan) {
    CreateStreamJobResult result = new StreamGraphBuilder(streamEngine, importManager)
        .createStreamGraph(plan.getStreamQueries());

    List<SqlDDLStatement> statements = new StreamTableDDLBuilder()
        .create(result.getCreatedTables(), true);
    statements.addAll(new ViewDDLBuilder()
        .create(plan.getDatabaseQueries()));

    return new ExecutionPlan(statements, result.getStreamQueries(), plan.getSchema());
  }
}
