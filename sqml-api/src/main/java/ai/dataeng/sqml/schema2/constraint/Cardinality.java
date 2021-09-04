package ai.dataeng.sqml.schema2.constraint;

import ai.dataeng.sqml.schema2.ArrayType;
import ai.dataeng.sqml.schema2.Type;
import com.google.common.base.Preconditions;

public class Cardinality implements Constraint {

    private final int min;
    private final int max;

    public Cardinality(int min, int max) {
        Preconditions.checkArgument(min>=0);
        Preconditions.checkArgument(max>=min);
        this.min = min;
        this.max = max;
    }

    @Override
    public boolean satisfies(Object value) {
        Preconditions.checkArgument(value.getClass().isArray());
        int length = ((Object[])value).length;
        return length>=min && length<=max;
    }

    @Override
    public boolean appliesTo(Type type) {
        return type instanceof ArrayType;
    }
}
