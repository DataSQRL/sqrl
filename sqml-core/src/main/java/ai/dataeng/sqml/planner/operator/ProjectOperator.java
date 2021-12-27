package ai.dataeng.sqml.planner.operator;

import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.LogicalPlanImpl;
import ai.dataeng.sqml.planner.operator.relation.ColumnReferenceExpression;
import ai.dataeng.sqml.planner.operator.relation.RowExpression;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Maps input rows onto output rows based on the provided projections.
 * Projections compute additional column values, drop columns, or rename columns.
 */
public class ProjectOperator extends LogicalPlanImpl.RowNode<LogicalPlanImpl.RowNode> {

    Map<ColumnReferenceExpression, RowExpression> projections;

    public ProjectOperator(LogicalPlanImpl.RowNode input, Map<ColumnReferenceExpression, RowExpression> projections) {
        super(input);
        this.projections = projections;
    }

    @Override
    public Column[][] getOutputSchema() {
        return new Column[][]{projections.keySet().stream().map(e->e.getColumn())
            .collect(Collectors.toList()).toArray(new Column[0])};
//        return new LogicalPlan.Column[][]{projections.keySet().toArray(new LogicalPlan.Column[0])};
    }

}
