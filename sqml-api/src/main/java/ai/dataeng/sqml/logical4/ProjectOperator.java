package ai.dataeng.sqml.logical4;

import java.util.List;

public class ProjectOperator extends LogicalPlan.RowNode<LogicalPlan.RowNode> {

    public ProjectOperator(LogicalPlan.RowNode input) {
        super(input);
    }

    @Override
    public LogicalPlan.Column[][] getOutputSchema() {
        return new LogicalPlan.Column[0][];
    }
}
