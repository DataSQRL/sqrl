package com.datasqrl.schema.input;

import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSchemaVisitor;

public class JsonTableSchema implements TableSchema {

  @Override
  public <R, C> R accept(TableSchemaVisitor<R, C> visitor, C context) {
    return visitor.accept(this, context);
  }
}
