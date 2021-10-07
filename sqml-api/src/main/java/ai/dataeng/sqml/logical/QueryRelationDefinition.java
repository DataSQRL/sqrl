package ai.dataeng.sqml.logical;

import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import java.util.List;
import lombok.Getter;

@Getter
public class QueryRelationDefinition extends RelationDefinition {

  private final RelationType relation;
  private final RelationIdentifier relationIdentifier;
  private final RelationDefinition parentRelation;

  public QueryRelationDefinition(RelationType relation, RelationIdentifier relationIdentifier,
      RelationDefinition parentRelation) {
    super();
    this.relation = relation;
    this.relationIdentifier = relationIdentifier;
    this.parentRelation = parentRelation;
  }
//
//  public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
//    return visitor.visitQueryRelationDefinition(this, context);
//  }

  @Override
  public List<Field> getFields() {
    return relation.getFields();
  }

  @Override
  public QualifiedName getRelationName() {
    return relationIdentifier.getName();
  }

  public RelationIdentifier getRelationIdentifier() {
    return relationIdentifier;
  }

  @Override
  public List<Field> getContextKey() {
    return parentRelation.getPrimaryKeys();
  }

  @Override
  protected List<Field> getPrimaryKeys() {
    return null; //todo: get group by definitions
  }
}