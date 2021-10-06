package ai.dataeng.sqml.schema2;

import ai.dataeng.sqml.schema2.name.NamePath;
import com.google.common.base.Preconditions;
import lombok.NonNull;

public class TypeHelper {

    public static<F extends TypedField> F getNestedField(@NonNull RelationType<F> relation, @NonNull NamePath path) {
        Preconditions.checkArgument(path.getLength()>0);
        RelationType<F> base = relation;
        F field = null;
        for (int i = 0; i < path.getLength(); i++) {
            if (i>0) base = asRelation(field.getType());
            field = base.getFieldByName(path.get(i));
            if (field == null) throw new IllegalArgumentException(String.format("Not a valid path [%s] within type hierarchy [%s]",path, relation));
        }
        return field;
    }

    public static Type getNestedType(RelationType<? extends TypedField> relation, NamePath path) {
        if (path.getLength()==0) return relation;
        return getNestedField(relation, path).getType();
    }

    public static RelationType getNestedRelation(RelationType<? extends TypedField> relation, NamePath path) {
        return asRelation(getNestedType(relation, path));
    }

    public static RelationType asRelation(Type type) {
        if (type instanceof ArrayType) {
            type = ((ArrayType) type).getSubType();
        }
        Preconditions.checkArgument(type instanceof RelationType,"Not a relation: %s", type);
        return (RelationType) type;
    }

}
