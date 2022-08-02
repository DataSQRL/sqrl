package ai.datasqrl.plan.global;

import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.queries.APIQuery;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

import java.util.List;

@Value
public class OptimizedDAG {

  List<WriteDB> streamTables;
  //TODO: Subscription tables
  List<ReadQuery> databaseQueries;

  @Value
  public static class WriteDB {

    final VirtualRelationalTable table;
    final RelNode relNode;

  }

  @Value
  public static class ReadQuery {

    final APIQuery query;
    final RelNode relNode;

  }

}
