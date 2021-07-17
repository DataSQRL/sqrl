package ai.dataeng.sqml.schema;

public class SchemaVisitor<R, C> {

  public R visitSchema(Schema schema, C context) {
    for (AbstractField abstractField : schema.getFields()) {
      abstractField.accept(this, context);
    }
    return null;
  }

  public R visitField(SchemaField schemaField, C context) {
    return null;
  }

  public R visitObject(SchemaObject schemaObject, C context) {
    for (AbstractField abstractField : schemaObject.getFields()) {
      abstractField.accept(this, context);
    }
    return null;
  }
}
