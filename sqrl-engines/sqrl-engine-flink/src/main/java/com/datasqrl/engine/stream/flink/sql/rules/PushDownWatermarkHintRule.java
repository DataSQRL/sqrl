package com.datasqrl.engine.stream.flink.sql.rules;

import com.datasqrl.calcite.schema.ExpandTableMacroRule;
import com.datasqrl.calcite.schema.ImmutableExpandTableMacroConfig;
import com.datasqrl.plan.hints.SqrlHint;
import com.datasqrl.plan.hints.WatermarkHint;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.Config;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;

public class PushDownWatermarkHintRule extends RelRule<PushDownWatermarkHintRule.Config>
    implements TransformationRule {

  protected PushDownWatermarkHintRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalProject topProject = call.rel(0);
    RelNode input = call.rel(1);

    Optional<WatermarkHint> watermarkHint = SqrlHint.fromRel(topProject, WatermarkHint.CONSTRUCTOR);
    if (watermarkHint.isEmpty()) {
      return;
    }

    RelBuilder relBuilder = call.builder();

    if (input instanceof Project) {
      Project bottomProject = (Project) input;
      final List<RexNode> newProjects =
          RelOptUtil.pushPastProjectUnlessBloat(topProject.getProjects(),
              bottomProject, 0);
      if (newProjects == null) {
        throw new RuntimeException("Could not merge projections for watermark determination. Query too complex.");
      }
      relBuilder.push(bottomProject.getInput());
      relBuilder.project(newProjects, topProject.getRowType().getFieldNames());
      //TODO: Could there be more hints?
      relBuilder.hints(watermarkHint.get().getHint());
    } else if (input instanceof TableScan) {
      TableScan tableScan = (TableScan) input;
      //rewrite table scan but without the hints
      RelNode scanWithoutHints = tableScan.withHints(List.of());
      relBuilder.push(scanWithoutHints);
      relBuilder.push(topProject);
    } else {
      throw new RuntimeException("Unexpected rel type");
    }

    call.transformTo(relBuilder.build());
  }

  private List<RexNode> allNodes(RelBuilder builder, RelNode node) {
    return IntStream.range(0, node.getRowType().getFieldCount())
        .mapToObj(i->builder.getRexBuilder().makeInputRef(node, i))
        .collect(Collectors.toList());
  }

  @Value.Immutable
  public interface PushDownWatermarkHintConfig extends RelRule.Config {
     PushDownWatermarkHintRule.Config DEFAULT = ImmutablePushDownWatermarkHintConfig.builder()
         .relBuilderFactory(RelFactories.LOGICAL_BUILDER)
         .description("PushDownWatermarkHintRule")
         .operandSupplier(b0 ->
             b0.operand(LogicalProject.class).oneInput(
                 b1 -> b1.operand(RelNode.class).anyInputs()))
        .build();

    @Override default RelOptRule toRule() {
      return new PushDownWatermarkHintRule(this);
    }
  }
}
