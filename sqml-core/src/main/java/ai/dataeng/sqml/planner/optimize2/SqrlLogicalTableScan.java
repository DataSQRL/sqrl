package ai.dataeng.sqml.planner.optimize2;

import ai.dataeng.sqml.planner.Column;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

public class SqrlLogicalTableScan
    extends TableScan
    implements SqrlLogicalNode {

  private final List<Column> primaryKeys;

  public SqrlLogicalTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      List<RelHint> hints, RelOptTable table,
      List<Column> primaryKeys) {
    super(cluster, traitSet, hints, table);
    this.primaryKeys = primaryKeys;
  }

  public List<Column> getImplicitKeys() {
    return primaryKeys;
  }
}
