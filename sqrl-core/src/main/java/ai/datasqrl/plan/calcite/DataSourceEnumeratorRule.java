package ai.datasqrl.plan.calcite;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class DataSourceEnumeratorRule extends RelOptRule {

  public DataSourceEnumeratorRule() {
    super(operand(LogicalTableScan.class, any()));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalTableScan scan = call.rel(0);

    call.transformTo(
        new SqrlEnumerableTableScan(
            scan.getCluster(),
            scan.getTraitSet(),
            scan.getHints(),
            scan.getTable()
        )
    );
  }
}