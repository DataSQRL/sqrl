package ai.dataeng.sqml.logical4;

import java.util.List;

public class ProjectOperator extends LogicalPlan.RowNode {

    LogicalPlan.RowNode input;

    @Override
    List<LogicalPlan.RowNode> getInputs() {
        return List.of(input);
    }

    @Override
    LogicalPlan.Column[][] getOutputSchema() {
        return new LogicalPlan.Column[0][];
    }
}
