package org.apache.calcite.rules;

import ai.dataeng.sqml.planner.DatasetOrTable;
import ai.dataeng.sqml.planner.optimize2.SqrlLogicalTableScan;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.schema.SqrlCalciteTable;
import org.apache.calcite.rules.RuleStub.Config;

public class RuleStub
      extends RelRule<Config>
      implements TransformationRule {

    public RuleStub(Config config) {
        super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      TableScan tableScan = call.rel(0);
      SqrlCalciteTable table = tableScan.getTable().unwrap(SqrlCalciteTable.class);
      DatasetOrTable sqrlTable = table.getTable();
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {
      Config DEFAULT = EMPTY
          .withOperandSupplier(b ->
//              b.exactly(new RelOptRuleOperand())
              b.operand(TableScan.class).anyInputs())
          .as(Config.class);

      @Override default RuleStub toRule() {
        return new RuleStub(this);
      }
    }
  }