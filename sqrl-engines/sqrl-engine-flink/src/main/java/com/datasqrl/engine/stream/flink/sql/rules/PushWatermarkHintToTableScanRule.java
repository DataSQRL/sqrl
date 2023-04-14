package com.datasqrl.engine.stream.flink.sql.rules;

import com.datasqrl.plan.hints.SqrlHint;
import com.datasqrl.plan.hints.WatermarkHint;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBeans;

public class PushWatermarkHintToTableScanRule extends RelRule<PushWatermarkHintToTableScanRule.Config>
    implements TransformationRule {

  protected PushWatermarkHintToTableScanRule() {
    super(PushWatermarkHintToTableScanRule.Config.DEFAULT);
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
  public interface Config extends RelRule.Config {
    PushWatermarkHintToTableScanRule.Config DEFAULT = EMPTY
        .withOperandSupplier(b0 ->
            b0.operand(LogicalProject.class).oneInput(
                b1 -> b1.operand(TableScan.class).anyInputs()))
        .withDescription("PushWatermarkHintToTableScanRule")
        .as(PushWatermarkHintToTableScanRule.Config.class);

    @Override default PushWatermarkHintToTableScanRule toRule() {
      return new PushWatermarkHintToTableScanRule();
    }

    /** Whether to include outer joins, default false. */
    @ImmutableBeans.Property
    @ImmutableBeans.BooleanDefault(false)
    boolean isIncludeOuter();

    /** Sets {@link #isIncludeOuter()}. */
    PushWatermarkHintToTableScanRule.Config withIncludeOuter(boolean includeOuter);
  }
}
