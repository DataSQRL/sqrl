package ai.datasqrl.physical.database;

import ai.datasqrl.physical.database.ddl.SqlDDLStatement;
import ai.datasqrl.plan.global.OptimizedDAG;

import java.util.List;

public class ViewDDLBuilder {

  public List<SqlDDLStatement> create(List<OptimizedDAG.ReadQuery> databaseQueries) {
    //todo
    return List.of();
  }
}
