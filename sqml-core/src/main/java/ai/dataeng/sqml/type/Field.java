package ai.dataeng.sqml.type;


import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.name.Name;
import java.io.Serializable;
import java.util.Optional;

public interface Field extends Serializable {
    public Name getName();

    public default String getCanonicalName() {
        return getName().getCanonical();
    }

    public default Type getType() {
        return null;
    }

    default boolean isHidden() {
        return false;
    }

    public Optional<QualifiedName> getAlias();

    Field withAlias(QualifiedName alias);

    default QualifiedName getQualifiedName() {
        if (unbox(getType()) instanceof RelationType) {
            Optional<Field> field = ((RelationType) unbox(getType())).getField("parent");
            if (field.isPresent() && field.get() instanceof TypedField) {
                QualifiedName parentName = ((TypedField)field.get()).getQualifiedName();

                return parentName.append(getName());
            }
        }

        return QualifiedName.of(getName());
    }

    static Type unbox(Type type) {
        if (type instanceof RelationType) {
            return type;
        }
        if (type instanceof ArrayType) {
            ArrayType arr = (ArrayType) type;
            return unbox(arr.getSubType());
        }
        return null;
    }
}
