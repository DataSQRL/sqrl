package ai.dataeng.sqml.logical3;

import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.TypedField;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.name.Name;
import java.util.Optional;

public class SubscriptionField implements TypedField {

  @Override
  public Name getName() {
    return null;
  }

  @Override
  public Field withAlias(QualifiedName alias) {
    return null;
  }

  @Override
  public Type getType() {
    return null;
  }

  @Override
  public Optional<QualifiedName> getAlias() {
    return Optional.empty();
  }
}