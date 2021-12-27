package ai.dataeng.sqml.planner.optimize;

import ai.dataeng.sqml.planner.LogicalPlanImpl;
import lombok.Value;

import java.util.List;

public interface LogicalPlanOptimizer {

    Result optimize(LogicalPlanImpl logicalPlan);


    @Value
    public static class Result {

        private final List<LogicalPlanImpl.Node> streamLogicalPlan;

        //private final List<LogicalPlan.Node> batchLogicalPlan;

        private final List<MaterializeSource> readLogicalPlan;

    }

}
