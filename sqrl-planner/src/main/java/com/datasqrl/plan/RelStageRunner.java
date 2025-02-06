package com.datasqrl.plan;

import static java.util.Objects.requireNonNull;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.flink.calcite.shaded.com.google.common.collect.ImmutableList;

public class RelStageRunner {

  public static RelNode runStage(OptimizationStage stage, RelNode relNode, RelOptPlanner planner) {
    var outputTraits = planner.emptyTraitSet();
    outputTraits = stage.getTrait().map(outputTraits::replace).orElse(outputTraits);
    return runStage(stage.getIndex(), outputTraits, relNode, planner);
  }

  protected static RelNode runStage(int ruleSetIndex, RelTraitSet requiredOutputTraits, RelNode rel,
      RelOptPlanner planner) {
    var program = OptimizationStage.getAllPrograms().get(ruleSetIndex);
    return program.run(requireNonNull(planner, "planner"),
        rel, requiredOutputTraits, ImmutableList.of(),
        ImmutableList.of());
  }
}
