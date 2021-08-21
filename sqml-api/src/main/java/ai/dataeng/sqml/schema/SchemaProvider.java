package ai.dataeng.sqml.schema;

import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.RelationType;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SchemaProvider {
  private final Map<String, Schema> schemas;

  public SchemaProvider(Map<String, Schema> schemas) {
    this.schemas = schemas;
  }

  public Schema get(String name) {
    return schemas.get(name);
  }

  public static Builder newSchemaProvider() {
    return new Builder();
  }

  public Optional<RelationType> get(QualifiedName name) {
    return null;
  }

  public static class Builder {
    private Map<String, Schema> schemas = new HashMap<>();

    public Builder addSchema(String name, Schema schema) {
      schemas.put(name, schema);
      return this;
    }

    public SchemaProvider build() {
      return new SchemaProvider(schemas);
    }
  }
}
