package ai.dataeng.sqml.logical;

import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.tree.QualifiedName;
import java.util.List;

public class InlineJoinFieldDefinition extends RelationDefinition {

  @Override
  public List<Field> getFields() {
    return null;
  }

  @Override
  public QualifiedName getRelationName() {
    return null;
  }

  @Override
  public RelationIdentifier getRelationIdentifier() {
    return null;
  }

  @Override
  protected List<Field> getPrimaryKeys() {
    return null;
  }
}
