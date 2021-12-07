package ai.dataeng.sqml.logical4;

import java.util.List;

public class AggregateOperator extends LogicalPlan.RowNode<LogicalPlan.RowNode> {

    public AggregateOperator(LogicalPlan.RowNode input) {
        super(input);
    }

    @Override
    public LogicalPlan.Column[][] getOutputSchema() {
        return new LogicalPlan.Column[0][];
    }
}
