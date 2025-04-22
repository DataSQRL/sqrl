package com.datasqrl.engine.stream.flink.sql.rules;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

public class ShapeBushyCorrelateJoinRule extends RelRule<ShapeBushyCorrelateJoinRule.Config>
    implements TransformationRule {

  protected ShapeBushyCorrelateJoinRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall relOptRuleCall) {
	  LogicalCorrelate top = relOptRuleCall.rel(0);
    var left = relOptRuleCall.rel(1);
    var right = relOptRuleCall.rel(2);

    var builder = relOptRuleCall.builder()
        .transform(config -> config.withSimplify(false).withPruneInputOfAggregate(false)
            .withBloat(-10000).withSimplifyLimit(false).withPushJoinCondition(false)
        );

    var relNode = builder
        .push(left)
        .project(allNodes(builder, builder.peek()),
            builder.peek().getRowType().getFieldNames(), true)
        .push(right)
        .correlate(top.getJoinType(), top.getCorrelationId())
        .build();

    relOptRuleCall.transformTo(relNode);
  }

  private List<RexNode> allNodes(RelBuilder builder, RelNode node) {
    return IntStream.range(0, node.getRowType().getFieldCount())
        .mapToObj(i->builder.getRexBuilder().makeInputRef(node, i))
        .collect(Collectors.toList());
  }


  /** Rule configuration. */
  @Value.Immutable
  public interface ShapeBushyCorrelateJoinRuleConfig extends RelRule.Config {
    public ShapeBushyCorrelateJoinRule.Config DEFAULT = ImmutableShapeBushyCorrelateJoinRuleConfig.builder()
        .relBuilderFactory(RelFactories.LOGICAL_BUILDER)
        .operandSupplier(b0 ->
            b0.operand(LogicalCorrelate.class).inputs(
                b1 -> b1.operand(LogicalCorrelate.class).anyInputs(),
                b2 -> b2.operand(RelNode.class).anyInputs()))
        .description("ShapeBushyCorrelateJoinRule")
        .build();

    @Override default ShapeBushyCorrelateJoinRule toRule() {
      return new ShapeBushyCorrelateJoinRule(this);
    }
  }
}
