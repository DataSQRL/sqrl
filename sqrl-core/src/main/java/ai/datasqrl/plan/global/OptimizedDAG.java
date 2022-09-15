package ai.datasqrl.plan.global;

import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.queries.APIQuery;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;
import java.util.Optional;

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
    final Optional<Integer> timestampIdx;

    public String getNameId() {
      return table.getNameId();
    }

    public RelDataType getRowType() {
      return table.getRowType();
    }

    public int getNumPrimaryKeys() {
      return table.getNumPrimaryKeys();
    }

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
