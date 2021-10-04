package ai.dataeng.sqml.logical;

import ai.dataeng.sqml.analyzer.Field;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import java.util.List;
import lombok.AllArgsConstructor;

/**
 * A child on this branch had a new field or relation.
 */
@AllArgsConstructor
public class ExtendedChildRelationDefinition extends RelationDefinition {

  RelationDefinition parent;

  @Override
  public List<Field> getFields() {
    return parent.getFields();
  }

  @Override
  public QualifiedName getRelationName() {
    return parent.getRelationName();
  }

  @Override
  public RelationIdentifier getRelationIdentifier() {
    return parent.getRelationIdentifier();
  }

  public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitExtendedChild(this, context);
  }

  @Override
  protected List<Field> getPrimaryKeys() {
    return parent.getPrimaryKeys();
  }
}
