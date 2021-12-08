package ai.dataeng.sqml.logical4;

import ai.dataeng.sqml.relation.RowExpression;
import ai.dataeng.sqml.relation.VariableReferenceExpression;
import java.util.List;
import java.util.Map;
import lombok.Value;

/**
 * Maps input rows onto output rows based on the provided projections.
 * Projections compute additional column values, drop columns, or rename columns.
 */
public class ProjectOperator extends LogicalPlan.RowNode<LogicalPlan.RowNode> {

    Map<LogicalPlan.Column, RowExpression> projections;

    public ProjectOperator(LogicalPlan.RowNode input, Map<LogicalPlan.Column, RowExpression> projections) {
        super(input);
        this.projections = projections;
    }

    @Override
    public LogicalPlan.Column[][] getOutputSchema() {
        return new LogicalPlan.Column[][]{projections.keySet().toArray(new LogicalPlan.Column[0])};
    }

}
