package ai.dataeng.sqml.schema2;


import static ai.dataeng.sqml.logical3.LogicalPlan.Builder.unbox;

import ai.dataeng.sqml.tree.QualifiedName;
import java.io.Serializable;
import java.util.Optional;
import ai.dataeng.sqml.tree.name.Name;

public interface Field extends Serializable {
    public Name getName();

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
}
