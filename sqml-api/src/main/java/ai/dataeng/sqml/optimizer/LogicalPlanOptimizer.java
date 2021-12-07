package ai.dataeng.sqml.optimizer;

import ai.dataeng.sqml.logical4.LogicalPlan;
import lombok.Value;

import java.util.List;

public interface LogicalPlanOptimizer {

    Result optimize(LogicalPlan logicalPlan);


    @Value
    public static class Result {

        private final List<LogicalPlan.Node> streamLogicalPlan;

        //private final List<LogicalPlan.Node> batchLogicalPlan;

        private final List<MaterializeSource> readLogicalPlan;

    }

}
