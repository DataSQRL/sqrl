/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import com.datasqrl.canonicalizer.Name;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.rel.type.RelDataType;

@Getter
public class Column extends Field {

  private final RelDataType type;
  private final boolean isVisible;
  private final boolean nullable;
  @Setter
  private String vtName;

  public Column(Name name, int version, boolean isVisible, RelDataType type) {
    super(name, version);
    this.isVisible = isVisible;
    this.type = type;
    this.nullable = type.isNullable();
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
