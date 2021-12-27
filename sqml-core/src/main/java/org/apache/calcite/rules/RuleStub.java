package org.apache.calcite.rules;

import ai.dataeng.sqml.planner.DatasetOrTable;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.CalciteTable;
import org.apache.calcite.rules.RuleStub.Config;

public class RuleStub
      extends RelRule<Config>
      implements TransformationRule {

    public RuleStub(Config config) {
        super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      LogicalTableScan tableScan = call.rel(0);
      CalciteTable table = tableScan.getTable().unwrap(CalciteTable.class);
      DatasetOrTable sqrlTable = table.getTable();
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {
      Config DEFAULT = EMPTY
          .withOperandSupplier(b ->
              b.operand(TableScan.class).anyInputs())
          .as(Config.class);

      @Override default RuleStub toRule() {
        return new RuleStub(this);
      }
    }
  }