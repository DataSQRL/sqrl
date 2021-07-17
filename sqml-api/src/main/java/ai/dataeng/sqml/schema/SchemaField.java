package ai.dataeng.sqml.schema;

import java.util.ArrayList;
import java.util.List;

public class SchemaField extends AbstractField {

  private final String name;
  private final SchemaObjectType type;
  private List<Validator> validators = new ArrayList<>();

  protected SchemaField(String name, SchemaObjectType type) {
    this.name = name;
    this.type = type;
  }

  public static SchemaField newString(String name) {
    return new SchemaField(name, SchemaObjectType.STRING);
  }

  public static SchemaField newInteger(String name) {
    return new SchemaField(name, SchemaObjectType.STRING);
  }

  public SchemaField validator(Validator validator) {
    this.validators.add(validator);
    return this;
  }

  @Override
  public <R, C> R accept(SchemaVisitor<R, C> visitor, C context) {
    return visitor.visitField(this, context);
  }

  public String getName() {
    return name;
  }

  public SchemaObjectType getType() {
    return type;
  }

  public List<Validator> getValidators() {
    return validators;
  }

  public static enum SchemaObjectType {
    STRING, NUMBER, NULL
  }
}
