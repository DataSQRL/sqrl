package com.datasqrl.io.tables;

public interface TableSchema {

  <R, C> R accept(TableSchemaVisitor<R, C> visitor, C context);
}
