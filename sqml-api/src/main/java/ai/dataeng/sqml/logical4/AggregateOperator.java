package ai.dataeng.sqml.logical4;

import java.util.List;

public class AggregateOperator extends LogicalPlan.RecordNode {

    LogicalPlan.RecordNode input;

    @Override
    List<LogicalPlan.RecordNode> getInputs() {
        return List.of(input);
    }
}
