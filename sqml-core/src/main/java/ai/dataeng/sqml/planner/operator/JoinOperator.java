package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.planner.LogicalPlanImpl;
import ai.dataeng.sqml.planner.operator.relation.RowExpression;

import java.util.Arrays;
import java.util.List;

/**
 * Joins two input streams (left and right) on the provided {@link JoinOperator#joinPredicate} and produces
 * a result stream that combines the rows from both sides if they satisfy the join predicate.
 */
public class JoinOperator extends LogicalPlanImpl.RowNode<LogicalPlanImpl.RowNode> {

    final RowExpression joinPredicate;
    final LogicalPlanImpl.Column[][] outputSchema;

    public JoinOperator(LogicalPlanImpl.RowNode left, LogicalPlanImpl.RowNode right, RowExpression joinPredicate) {
        super(List.of(left, right));
        this.joinPredicate = joinPredicate;

        //Combine the schemas
        LogicalPlanImpl.Column[][] leftSchema = left.getOutputSchema();
        LogicalPlanImpl.Column[][] rightSchema = left.getOutputSchema();
        outputSchema = Arrays.copyOf(leftSchema, leftSchema.length + rightSchema.length);
        System.arraycopy(rightSchema,0,outputSchema,leftSchema.length,rightSchema.length);
    }

    @Override
    public LogicalPlanImpl.Column[][] getOutputSchema() {
        return outputSchema;
    }

}
