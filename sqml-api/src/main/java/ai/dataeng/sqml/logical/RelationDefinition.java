package ai.dataeng.sqml.logical;

import ai.dataeng.sqml.analyzer.Field;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.RelationType;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import java.util.List;

public abstract class RelationDefinition extends RelationType {
    public abstract List<Field> getFields();
    public abstract QualifiedName getRelationName();
    public abstract RelationIdentifier getRelationIdentifier();

    public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
        return visitor.visitRelationDefinition(this, context);
    }

    public List<Field> getContextKey() {
        return List.of();
    }

    protected abstract List<Field> getPrimaryKeys();
}
