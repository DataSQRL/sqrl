package ai.dataeng.sqml.logical;

import ai.dataeng.sqml.analyzer.Field;
import ai.dataeng.sqml.tree.DistinctAssignment;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.RelationType;
import ai.dataeng.sqml.type.SqmlTypeVisitor;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;

@Getter
public class DistinctRelationDefinition extends RelationDefinition {
  private final QualifiedName qualifiedName;
  private final DistinctAssignment expression;
  private final RelationDefinition parent;
  private final RelationIdentifier relationIdentifier;

  public DistinctRelationDefinition(QualifiedName qualifiedName, DistinctAssignment expression,
      RelationDefinition parent, RelationIdentifier relationIdentifier) {
    this.qualifiedName = qualifiedName;
    this.expression = expression;
    this.parent = parent;
    this.relationIdentifier = relationIdentifier;
  }

  public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitDistinctRelation(this, context);
  }

  @Override
  protected List<Field> getPrimaryKeys() {
    List<Field> fields = new ArrayList<>();
    Set<String> fieldSet = expression.getFields()
        .stream().map(e->e.getValue())
        .collect(Collectors.toSet());

    //Todo: not right, could be an expression
    for (Field field : parent.getFields()) {
      if (fieldSet.contains(field.getName().get())) {
        fields.add(field);
      }
    }

    return fields; //todo add
  }

  @Override
  public List<Field> getFields() {
    //Distinct table definitions only inherit some fields
    return parent.getFields().stream()
        .filter(this::allowableFields)
        .collect(Collectors.toList());
  }

  @Override
  public QualifiedName getRelationName() {
    return qualifiedName;
  }

  public RelationIdentifier getRelationIdentifier() {
    return relationIdentifier;
  }

  private boolean allowableFields(Field field) {
    //Todo: Restrict more fields from a DISTINCT ON statement
    return !(field.getType() instanceof RelationType);
  }
}
