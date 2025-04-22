package com.datasqrl.util;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import com.datasqrl.canonicalizer.Name;

public interface RelDataTypeBuilder {

  public default RelDataTypeBuilder add(Name name, RelDataType type) {
    return add(name.getDisplay(), type);
  }

  public RelDataTypeBuilder add(String name, RelDataType type);

  public default RelDataTypeBuilder add(Name name, RelDataType type, boolean nullable) {
    return add(name.getDisplay(), type, nullable);
  }

  public RelDataTypeBuilder add(String name, RelDataType type, boolean nullable);

  public RelDataTypeBuilder add(RelDataTypeField field);

  public default RelDataTypeBuilder addAll(Iterable<RelDataTypeField> fields) {
    for (RelDataTypeField field : fields) {
      add(field);
    }
    return this;
  }

  public int getFieldCount();

  public RelDataType build();

}
