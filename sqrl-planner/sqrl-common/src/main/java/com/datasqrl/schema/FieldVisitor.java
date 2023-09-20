/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

public interface FieldVisitor<R, C> {
  R visit(Column column, C context);
  R visit(Relationship column, C context);
}
