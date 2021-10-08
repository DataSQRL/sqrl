package ai.dataeng.sqml.logical;

import ai.dataeng.sqml.analyzer.Scope;
import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.tree.CreateSubscription;
import ai.dataeng.sqml.tree.QualifiedName;
import java.util.List;

public class SubscriptionDefinition extends RelationDefinition {

  public SubscriptionDefinition(CreateSubscription subscription, Scope queryScope) {

  }

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
