package ai.dataeng.sqml.relation;

import ai.dataeng.sqml.schema2.Type;

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
