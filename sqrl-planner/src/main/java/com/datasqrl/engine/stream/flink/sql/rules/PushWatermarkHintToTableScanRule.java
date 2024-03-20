package com.datasqrl.engine.stream.flink.sql.rules;

import com.datasqrl.plan.hints.SqrlHint;
import com.datasqrl.plan.hints.WatermarkHint;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

public class PushWatermarkHintToTableScanRule extends RelRule<PushWatermarkHintToTableScanRule.Config>
    implements TransformationRule {

  protected PushWatermarkHintToTableScanRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalProject topProject = call.rel(0);
    TableScan tableScan = call.rel(1);

    Optional<WatermarkHint> watermarkHint = SqrlHint.fromRel(topProject, WatermarkHint.CONSTRUCTOR);
    if (watermarkHint.isEmpty()) {
      return;
    }

    RelBuilder relBuilder = call.builder();

    //rewrite table scan but without the hints
    RelNode scanWithHints = tableScan.withHints(List.of(watermarkHint.get().getHint()));
    relBuilder.push(scanWithHints);
    relBuilder.push(topProject.withHints(List.of()));

    call.transformTo(relBuilder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface PushWatermarkHintToTableScanConfig extends RelRule.Config {
    PushWatermarkHintToTableScanRule.Config DEFAULT = ImmutablePushWatermarkHintToTableScanConfig.builder()
        .relBuilderFactory(RelFactories.LOGICAL_BUILDER)
        .operandSupplier(b0 ->
            b0.operand(LogicalProject.class).oneInput(
                b1 -> b1.operand(TableScan.class).anyInputs()))
        .description("PushWatermarkHintToTableScanRule")
        .build();

    @Override default PushWatermarkHintToTableScanRule toRule() {
      return new PushWatermarkHintToTableScanRule(this);
    }
  }
}
