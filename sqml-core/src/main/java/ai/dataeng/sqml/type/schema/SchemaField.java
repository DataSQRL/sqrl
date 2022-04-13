package ai.dataeng.sqml.type.schema;

import ai.dataeng.sqml.tree.name.Name;
import java.io.Serializable;

public interface SchemaField extends Serializable {

  public Name getName();
}
