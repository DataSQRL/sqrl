package com.datasqrl.plan.calcite.memory.rule;

import com.datasqrl.plan.calcite.memory.rel.InMemoryEnumerableTableScan;
import org.apache.calcite.adapter.enumerable.EnumerableTableScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

/**
 * Replaces LogicalTableScan with a DataSourceEnumerableTableScan for enumerable data sources.
 * <p>
 * Converter rules do not change semantics, only change from one calling convention to another.
 */
public class SqrlDataSourceToEnumerableConverterRule extends RelOptRule {

  public SqrlDataSourceToEnumerableConverterRule() {
    super(operand(EnumerableTableScan.class, any()));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final EnumerableTableScan scan = call.rel(0);

    call.transformTo(
        new InMemoryEnumerableTableScan(
            scan.getCluster(),
            scan.getTraitSet(),
            scan.getHints(),
            scan.getTable()
        )
    );
  }
}