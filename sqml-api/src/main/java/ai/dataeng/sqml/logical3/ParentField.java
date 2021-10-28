package ai.dataeng.sqml.logical3;

import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.TypedField;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import java.util.Optional;

/**
   * Field that resolve when resolving 'parent'
   */
public class ParentField implements TypedField {

  private final RelationType type;
  private final Optional<String> alias;

  public ParentField(RelationType type) {
    this(type, Optional.empty());
  }

  public ParentField(RelationType type, Optional<String> alias) {
    this.type = type;
    this.alias = alias;
  }

  @Override
  public Name getName() {
    return Name.of("parent", NameCanonicalizer.SYSTEM);
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public boolean isHidden() {
    return true;
  }

  @Override
  public Field withAlias(String alias) {
    return new ParentField(type, Optional.of(alias));
  }
}
