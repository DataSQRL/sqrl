package ai.dataeng.sqml.planner.optimize2;

import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Table;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;

public class SqrlLogicalTableScan
    extends TableScan
    implements SqrlLogicalNode {

  private final ai.dataeng.sqml.planner.Table sqrlTable;

  public SqrlLogicalTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      List<RelHint> hints, RelOptTable table,
      ai.dataeng.sqml.planner.Table sqrlTable) {
    super(cluster, traitSet, hints, table);
    this.sqrlTable = sqrlTable;
  }

  public List<Column> getPrimaryKeys() {
    return sqrlTable.getPrimaryKeys();
  }

  public Table getSqrlTable() {
    return sqrlTable;
  }
}
