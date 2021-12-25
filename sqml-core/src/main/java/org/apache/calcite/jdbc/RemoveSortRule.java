package org.apache.calcite.jdbc;

import org.apache.calcite.jdbc.RemoveSortRule.Config;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.rules.TransformationRule;

public class RemoveSortRule
      extends RelRule<Config>
      implements TransformationRule {

    public RemoveSortRule(Config config) {
      super(config);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      call.transformTo(call.rel(0).getInput(0));
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {
      RemoveSortRule.Config DEFAULT = EMPTY
          .withOperandSupplier(b ->
              b.operand(LogicalSort.class).anyInputs())
          .as(RemoveSortRule.Config.class);

      @Override default RemoveSortRule toRule() {
        return new RemoveSortRule(this);
      }
    }
  }