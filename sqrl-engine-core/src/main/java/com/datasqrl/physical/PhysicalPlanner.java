package ai.datasqrl.physical;

import ai.datasqrl.config.util.StreamUtil;
import ai.datasqrl.plan.global.OptimizedDAG;
import lombok.AllArgsConstructor;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@AllArgsConstructor
public class PhysicalPlanner {

  RelBuilder relBuilder;

  public PhysicalPlan plan(OptimizedDAG plan) {
    List<PhysicalPlan.StagePlan> physicalStages = new ArrayList<>();
    for (int i = 0; i < plan.getStagePlans().size(); i++) {
      OptimizedDAG.StagePlan stagePlan = plan.getStagePlans().get(i);
      //1. Get all queries that sink into this stage
      List<OptimizedDAG.StageSink> inputs = StreamUtil.filterByClass(plan.getWriteQueries().stream().map(wq -> wq.getSink()), OptimizedDAG.StageSink.class)
              .filter(sink -> sink.getStage().equals(stagePlan.getStage())).collect(Collectors.toList());
      EnginePhysicalPlan physicalPlan = stagePlan.getStage().plan(stagePlan, inputs, relBuilder);
      physicalStages.add(new PhysicalPlan.StagePlan(stagePlan.getStage(), physicalPlan));
    }

    return new PhysicalPlan(physicalStages);
  }
}
