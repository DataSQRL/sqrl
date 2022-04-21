package ai.datasqrl.schema.type.schema;

import ai.datasqrl.parse.tree.name.Name;
import java.io.Serializable;

public interface SchemaField extends Serializable {

  Name getName();
}
