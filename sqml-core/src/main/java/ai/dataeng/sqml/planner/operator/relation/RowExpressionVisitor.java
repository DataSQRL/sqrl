package ai.dataeng.sqml.planner.operator.relation;

public interface RowExpressionVisitor<R, C> {
    R visitCall(CallExpression call, C context);

    R visitConstant(ConstantExpression literal, C context);

    R visitVariableReference(VariableReferenceExpression reference, C context);

    R visitSpecialForm(SpecialFormExpression specialForm, C context);
}
