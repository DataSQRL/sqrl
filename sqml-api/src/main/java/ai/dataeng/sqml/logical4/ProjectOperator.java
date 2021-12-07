package ai.dataeng.sqml.logical4;

import ai.dataeng.sqml.relation.RowExpression;
import ai.dataeng.sqml.relation.VariableReferenceExpression;
import java.util.List;
import java.util.Map;
import lombok.Value;

public class ProjectOperator extends LogicalPlan.RowNode<LogicalPlan.RowNode> {
    LogicalPlan.RowNode input;

    Map<VariableReferenceExpression, RowExpression> assignments;
    Locality locality;

    public ProjectOperator(LogicalPlan.RowNode input, Map<VariableReferenceExpression, RowExpression> assignments,
        Locality locality) {
        super(input);
    }

    @Override
    public LogicalPlan.Column[][] getOutputSchema() {
        return new LogicalPlan.Column[0][];
    }

    public enum Locality
    {
        UNKNOWN,
        LOCAL,
        REMOTE,
    }
}
