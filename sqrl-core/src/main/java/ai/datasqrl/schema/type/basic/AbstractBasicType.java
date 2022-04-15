package ai.datasqrl.schema.type.basic;

import ai.datasqrl.schema.type.SqmlTypeVisitor;

public abstract class AbstractBasicType<J> implements BasicType<J> {

    AbstractBasicType() {
    }

    @Override
    public BasicType parentType() {
        return null;
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractBasicType<?> that = (AbstractBasicType<?>) o;
        return getName().equals(that.getName());
    }


    @Override
    public String toString() {
        return getName();
    }

    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
        return visitor.visitBasicType(this, context);
    }
}
