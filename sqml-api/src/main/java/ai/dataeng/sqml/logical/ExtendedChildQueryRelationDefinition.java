package ai.dataeng.sqml.logical;

import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

@Getter
public class ExtendedChildQueryRelationDefinition extends RelationDefinition {

  private final String name;
  private final Node node;
  private final RelationDefinition parent;
  private final Field field;

  public ExtendedChildQueryRelationDefinition(String name, Node node,
      RelationDefinition parent, Field field) {
    super();
    this.name = name;
    this.node = node;
    this.parent = parent;
    this.field = field;
  }

//  public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
//    return visitor.visitExtendedChildQueryRelation(this, context);
//  }

  @Override
  protected List<Field> getPrimaryKeys() {
    return parent.getPrimaryKeys();
  }

  @Override
  public List<Field> getFields() {
    List<Field> fields = new ArrayList<>(parent.getFields());
    fields.add(field);
    return fields;
  }

  @Override
  public QualifiedName getRelationName() {
    return parent.getRelationName();
  }

  @Override
  public RelationIdentifier getRelationIdentifier() {
    return parent.getRelationIdentifier();
  }
}
