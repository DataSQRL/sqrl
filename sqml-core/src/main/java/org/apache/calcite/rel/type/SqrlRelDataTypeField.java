package org.apache.calcite.rel.type;

import ai.dataeng.sqml.parser.FieldPath;
import lombok.Getter;

@Getter
public class SqrlRelDataTypeField extends RelDataTypeFieldImpl {

  private final FieldPath path;

  public SqrlRelDataTypeField(String name, int index, RelDataType type, FieldPath path) {
    super(name, index, type);
    this.path = path;
  }
}
