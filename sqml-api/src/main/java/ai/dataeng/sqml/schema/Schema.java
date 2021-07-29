package ai.dataeng.sqml.schema;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class Schema extends SchemaObject {

  public Schema(String name, Set<AbstractField> fields, boolean allowAdditionalFields) {
    super(name, allowAdditionalFields, fields);
  }

  public static Builder newSchema(String name) {
    return new Builder(name);
  }

  public static class Builder extends ObjectBuilder {
    private String schemaName;

    public Builder(String name) {
      super(null);
      this.schemaName = name;
    }

    public String getName() {
      return schemaName;
    }

    @Override
    public Builder getBase() {
      return this;
    }

    @Override
    public ObjectBuilder getParent() {
      return this;
    }

    public Schema buildSchema() {
      return new Schema(schemaName, fields, allowAdditionalFields);
    }
  }

  public static class ObjectBuilder {
    protected String name;
    private Builder base;
    private ObjectBuilder parent;
    protected Set<AbstractField> fields = new HashSet<>();
    protected boolean allowAdditionalFields;

    protected ObjectBuilder(String name) {
      this.name = name;
    }

    public ObjectBuilder(String name, Builder base, ObjectBuilder parent) {
      this.name = name;
      this.base = base;
      this.parent = parent;
    }

    public ObjectBuilder object(String name) {
      return new ObjectBuilder(name, base, this);
    }

    public ObjectBuilder field(SchemaField schemaField) {
      this.fields.add(schemaField);
      return this;
    }

    public ObjectBuilder allowAdditionalFields(boolean allowAdditionalFields) {
      this.allowAdditionalFields = allowAdditionalFields;
      return this;
    }

    public Set<AbstractField> getFields() {
      return fields;
    }

    protected boolean allowAdditionalFields() {
      return allowAdditionalFields;
    }

    public Builder getBase() {
      return base;
    }

    public ObjectBuilder getParent() {
      return parent;
    }

    protected String getName() {
      return name;
    }

    public ObjectBuilder build() {
      getParent().fields.add(new SchemaObject(getName(), allowAdditionalFields(), getFields()));
      return getParent();
    }

    public Schema buildSchema() {
      return getBase().buildSchema();
    }
  }

  public <R, C> R accept(SchemaVisitor<R, C> visitor, C context) {
    return visitor.visitSchema(this, context);
  }
}
