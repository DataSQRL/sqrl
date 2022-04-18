package ai.datasqrl.physical;

import ai.datasqrl.physical.database.ddl.SqlDDLStatement;
import ai.datasqrl.plan.LogicalPlan;
import ai.datasqrl.plan.RelQuery;
import java.util.List;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;

public class Physicalizer {

  public ExecutionPlan plan(LogicalPlan plan) {
    List<SqlDDLStatement> databaseDDL = createDatabaseDDL(plan.getDatabaseQueries());
    StreamStatementSet streamQueries = createStreamJobGraph(plan.getStreamQueries());

    return new ExecutionPlan(databaseDDL, streamQueries, plan.getSchema());
  }


  private List<SqlDDLStatement> createDatabaseDDL(List<RelQuery> databaseQueries) {
    return null;
  }

  private StreamStatementSet createStreamJobGraph(List<RelQuery> streamQueries) {



    return null;
  }
}
