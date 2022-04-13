package ai.dataeng.sqml.type;

import ai.dataeng.sqml.type.schema.SchemaField;

public interface TypedField extends SchemaField {

  public Type getType();
}
