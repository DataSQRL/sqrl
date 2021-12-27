package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.LogicalPlanImpl;
import java.util.List;

/**
 * Combines two input streams into one output stream with a consolidated schema.
 *
 * TODO: Work in progress
 */
public class UnionOperator extends LogicalPlanImpl.RowNode<LogicalPlanImpl.RowNode> {

    public UnionOperator(LogicalPlanImpl.RowNode one, LogicalPlanImpl.RowNode two) {
        super(List.of(one,two));
    }

    @Override
    public Column[][] getOutputSchema() {
        return new Column[0][];
    }
}
