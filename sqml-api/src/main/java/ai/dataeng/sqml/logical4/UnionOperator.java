package ai.dataeng.sqml.logical4;

import java.util.List;

/**
 * Combines two input streams into one output stream with a consolidated schema.
 *
 * TODO: Work in progress
 */
public class UnionOperator extends LogicalPlan.RowNode<LogicalPlan.RowNode> {

    public UnionOperator(LogicalPlan.RowNode one, LogicalPlan.RowNode two) {
        super(List.of(one,two));
    }

    @Override
    public LogicalPlan.Column[][] getOutputSchema() {
        return new LogicalPlan.Column[0][];
    }
}
