package ai.dataeng.sqml.schema;

import java.util.Optional;
import java.util.Set;

public class SchemaObject extends AbstractField {

  private final String name;
  private final boolean allowAdditionalFields;
  private final Set<AbstractField> fields;

  public SchemaObject(String name, boolean allowAdditionalFields,
      Set<AbstractField> fields) {
    this.name = name;
    this.allowAdditionalFields = allowAdditionalFields;
    this.fields = fields;
  }
  @Override
  public <R, C> R accept(SchemaVisitor<R, C> visitor, C context) {
    return visitor.visitObject(this, context);
  }

  public String getName() {
    return name;
  }

  public boolean isAllowAdditionalFields() {
    return allowAdditionalFields;
  }

  public Set<AbstractField> getFields() {
    return fields;
  }

  public Optional<AbstractField> getField(String name) {
    for (AbstractField field : fields) {
      if (field.getName().equalsIgnoreCase(name)) {
        return Optional.of(field);
      }
    }

    return Optional.empty();
  }
}
