package ai.datasqrl.schema.type;

import ai.datasqrl.schema.type.schema.SchemaField;

public interface TypedField extends SchemaField {

  Type getType();
}
