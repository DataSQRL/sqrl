package com.datasqrl.calcite.util;

import lombok.Value;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.StructKind;

public class RelDataTypeFieldBuilder {

  private final RelDataTypeFactory.FieldInfoBuilder fieldBuilder;

  public RelDataTypeFieldBuilder(RelDataTypeFactory factory) {
    this.fieldBuilder = factory.builder().kind(StructKind.FULLY_QUALIFIED);
  }

  public RelDataTypeFieldBuilder add(String name, RelDataType type) {
    fieldBuilder.add(name, type);
    return this;
  }

  public RelDataTypeFieldBuilder add(String name, RelDataType type, boolean nullable) {
    fieldBuilder.add(name, type).nullable(nullable);
    return this;
  }

  public RelDataTypeFieldBuilder add(RelDataTypeField field) {
    //TODO: Do we need to do a deep clone or is this kosher since fields are immutable?
    fieldBuilder.add(field);
    return this;
  }

  public RelDataTypeFieldBuilder addAll(Iterable<RelDataTypeField> fields) {
    for (RelDataTypeField field : fields) {
      add(field);
    }
    return this;
  }

  public RelDataType build() {
    return fieldBuilder.build();
  }
}