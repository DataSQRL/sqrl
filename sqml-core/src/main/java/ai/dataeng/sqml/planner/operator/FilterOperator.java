package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.planner.LogicalPlanImpl;
import ai.dataeng.sqml.planner.LogicalPlanImpl.RowNode;
import ai.dataeng.sqml.planner.operator.relation.RowExpression;
import lombok.Value;

/**
 * Filters the input stream based on the provided predicate.
 */
@Value
public class FilterOperator extends LogicalPlanImpl.RowNode<LogicalPlanImpl.RowNode> {

    RowExpression predicate;

    public FilterOperator(RowNode input, RowExpression predicate) {
        super(input);
        this.predicate = predicate;
    }

    @Override
    public LogicalPlanImpl.Column[][] getOutputSchema() {
        //Filters do not change the records or their schema
        return getInput().getOutputSchema();
    }
}
