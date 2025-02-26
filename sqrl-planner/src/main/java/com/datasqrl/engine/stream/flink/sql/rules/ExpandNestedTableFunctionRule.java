package com.datasqrl.engine.stream.flink.sql.rules;

import com.datasqrl.schema.NestedRelationship;
import java.util.List;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

public class ExpandNestedTableFunctionRule extends RelRule<Config> implements TransformationRule {

  protected ExpandNestedTableFunctionRule(Config config) {
    super(config);
  }

  /**
   * Converts a NestedRelationship table function to an UNNEST call
   *
   * @param relOptRuleCall
   */
  @Override
  public void onMatch(RelOptRuleCall relOptRuleCall) {
    TableFunctionScan scan = relOptRuleCall.rel(0);

    RexCall call = (RexCall) scan.getCall();
    if (!(call.getOperator() instanceof SqlUserDefinedTableFunction)) {
      return;
    }

    SqlUserDefinedTableFunction tableFunction = (SqlUserDefinedTableFunction) call.getOperator();
    TableFunction function = tableFunction.getFunction();
    if (!(function instanceof NestedRelationship)) {
      return;
    }

    NestedRelationship relationship = (NestedRelationship) function;

    RelBuilder relBuilder =
        relOptRuleCall
            .builder()
            .push(LogicalValues.createOneRow(relOptRuleCall.builder().getCluster()))
            .project(call.getOperands())
            .uncollect(List.of(), false);

    RelNode relNode = relBuilder.build();
    relOptRuleCall.transformTo(relNode);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface ExpandNestedTableFunctionRuleConfig extends Config {
    public Config DEFAULT =
        ImmutableExpandNestedTableFunctionRuleConfig.builder()
            .relBuilderFactory(RelFactories.LOGICAL_BUILDER)
            .operandSupplier(b0 -> b0.operand(TableFunctionScan.class).anyInputs())
            .description("ExpandNestedTableFunctionRule")
            .build();

    @Override
    default ExpandNestedTableFunctionRule toRule() {
      return new ExpandNestedTableFunctionRule(this);
    }
  }
}
