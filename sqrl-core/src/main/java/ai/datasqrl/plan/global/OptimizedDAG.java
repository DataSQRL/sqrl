package ai.datasqrl.plan.global;

import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.queries.APIQuery;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

import java.util.List;

@Value
public class OptimizedDAG {

  List<MaterializeQuery> streamQueries;
  List<ReadQuery> databaseQueries;

  @Value
  public static class MaterializeQuery {

    final MaterializeSink sink;
    final RelNode relNode;

  }

  public interface MaterializeSink {

  }

  @Value
  public static class TableSink implements MaterializeSink {

    final VirtualRelationalTable table;

  }

  @Value
  public static class StreamSink implements MaterializeSink {



  }

  @Value
  public static class ReadQuery {

    final APIQuery query;
    final RelNode relNode;

  }

}
