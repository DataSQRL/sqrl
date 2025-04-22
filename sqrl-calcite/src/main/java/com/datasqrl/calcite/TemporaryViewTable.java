package com.datasqrl.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

import lombok.AllArgsConstructor;

/**
 * A table that is strictly just a reldatatype, used for planning expressions
 */
@AllArgsConstructor
public class TemporaryViewTable extends AbstractTable {
  RelDataType relDataType;

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return relDataType;
  }
}
