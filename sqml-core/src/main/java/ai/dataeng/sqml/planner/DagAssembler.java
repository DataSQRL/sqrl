package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.schema.Namespace;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.schema.SqrlCalciteTable;

@AllArgsConstructor
public class DagAssembler extends RelShuttleImpl {
  Namespace namespace;

  @Override
  public RelNode visit(TableScan scan) {
    return ((SqrlCalciteTable) scan.getRowType()).getSqrlTable().getRelNode();
  }
}
