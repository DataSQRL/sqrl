/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import com.datasqrl.canonicalizer.Name;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;

@Getter
public class Column extends Field {

  private final Name vtName;
  private final RelDataType type;
  private final boolean isVisible;
  private final boolean nullable;

  public Column(Name name, Name vtName, int version, boolean isVisible, RelDataType type) {
    super(name, version);
    this.vtName = vtName;
    this.isVisible = isVisible;
    this.type = type;
    this.nullable = type.isNullable();
  }

  @Override
  public String toString() {
    return super.toString();
  }

  public <R, C> R accept(FieldVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}
