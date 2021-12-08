package ai.dataeng.sqml.logical4;

import ai.dataeng.sqml.relation.RowExpression;

import java.util.Arrays;
import java.util.List;

/**
 * Joins two input streams (left and right) on the provided {@link JoinOperator#joinPredicate} and produces
 * a result stream that combines the rows from both sides if they satisfy the join predicate.
 */
public class JoinOperator extends LogicalPlan.RowNode<LogicalPlan.RowNode> {

    final RowExpression joinPredicate;
    final LogicalPlan.Column[][] outputSchema;

    public JoinOperator(LogicalPlan.RowNode left, LogicalPlan.RowNode right, RowExpression joinPredicate) {
        super(List.of(left, right));
        this.joinPredicate = joinPredicate;

        //Combine the schemas
        LogicalPlan.Column[][] leftSchema = left.getOutputSchema();
        LogicalPlan.Column[][] rightSchema = left.getOutputSchema();
        outputSchema = Arrays.copyOf(leftSchema, leftSchema.length + rightSchema.length);
        System.arraycopy(rightSchema,0,outputSchema,leftSchema.length,rightSchema.length);
    }

    @Override
    public LogicalPlan.Column[][] getOutputSchema() {
        return outputSchema;
    }

}
