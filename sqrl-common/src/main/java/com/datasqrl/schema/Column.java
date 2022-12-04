/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import com.datasqrl.name.Name;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;

@Getter
public class Column extends Field {

  private final Name shadowedName;
  final boolean isVisible;
  private final RelDataType type;
  boolean nullable;

  public Column(Name name, Name shadowedName, int version, boolean isVisible, RelDataType type) {
    super(name, version);
    this.shadowedName = shadowedName;
    this.isVisible = isVisible;
    this.type = type;
    this.nullable = type.isNullable();
  }

  @Override
  public String toString() {
    return super.toString();
  }

  @Override
  public FieldKind getKind() {
    return FieldKind.COLUMN;
  }

}
