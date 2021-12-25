package ai.dataeng.sqml.planner.operator.relation;

import ai.dataeng.sqml.type.Type;

public abstract class RowExpression
{
    public abstract Type getType();

    @Override
    public abstract boolean equals(Object other);

    @Override
    public abstract int hashCode();

    @Override
    public abstract String toString();

    public abstract <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context);
}
