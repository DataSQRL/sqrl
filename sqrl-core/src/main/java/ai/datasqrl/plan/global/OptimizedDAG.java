package ai.datasqrl.plan.global;

import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.queries.APIQuery;
import lombok.AllArgsConstructor;
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

    public String getName();

  }

  @Value
  @AllArgsConstructor
  public static class TableSink implements MaterializeSink {

    final String nameId;
    final int numPrimaryKeys;
    final RelDataType rowType;
    final Optional<Integer> timestampIdx;

    public TableSink(VirtualRelationalTable table, Optional<Integer> timestampIdx) {
      this(table.getNameId(),table.getNumPrimaryKeys(),table.getRowType(),timestampIdx);
    }

    @Override
    public String getName() {
      return getNameId();
    }
  }

  @Value
  public static class ExternalSink implements MaterializeSink {

    String name;

  }

  @Value
  public static class ReadQuery {

    final APIQuery query;
    final RelNode relNode;

  }

}
