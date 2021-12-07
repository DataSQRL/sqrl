package ai.dataeng.sqml.logical4;

import ai.dataeng.sqml.relation.CallExpression;
import ai.dataeng.sqml.relation.VariableReferenceExpression;
import java.util.List;
import java.util.Map;
import lombok.Value;

@Value
public class AggregateOperator extends LogicalPlan.RowNode<LogicalPlan.RowNode> {
    Map<VariableReferenceExpression, Aggregation> aggregations;
    List<VariableReferenceExpression> groupingKeys;

    public AggregateOperator(LogicalPlan.RowNode input, Map<VariableReferenceExpression, Aggregation> aggregations, List<VariableReferenceExpression> groupingKeys) {
        super(input);

        this.aggregations = aggregations;
        this.groupingKeys = groupingKeys;
    }

    @Override
    public LogicalPlan.Column[][] getOutputSchema() {
        return new LogicalPlan.Column[0][];
    }

    @Value
    public static class Aggregation {
        CallExpression call;
    }
}