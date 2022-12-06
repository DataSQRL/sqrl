package com.datasqrl.schema;

public interface FieldVisitor<R, C> {
  R visit(Column column, C context);
}
