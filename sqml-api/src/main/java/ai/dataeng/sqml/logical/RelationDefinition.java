package ai.dataeng.sqml.logical;

import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import java.util.List;
import lombok.Data;
import lombok.NonNull;

public abstract class RelationDefinition extends RelationType<Field> {
    public abstract List<Field> getFields();
    public abstract QualifiedName getRelationName();
    public abstract RelationIdentifier getRelationIdentifier();
    public List<Field> getContextKey() {
        return List.of();
    }
    protected abstract List<Field> getPrimaryKeys();
    public QualifiedName getPath() {
        return null;
    }
}
