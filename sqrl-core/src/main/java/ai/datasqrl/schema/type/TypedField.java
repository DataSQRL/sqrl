package ai.datasqrl.schema.type;

import ai.datasqrl.schema.input.SchemaField;

public interface TypedField extends SchemaField {

  Type getType();
}
