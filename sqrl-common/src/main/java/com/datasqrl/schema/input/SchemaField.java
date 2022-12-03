package com.datasqrl.schema.input;

import com.datasqrl.parse.tree.name.Name;
import java.io.Serializable;

public interface SchemaField extends Serializable {

  Name getName();
}
