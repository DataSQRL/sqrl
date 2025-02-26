package com.datasqrl.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;

public interface ModifiableTable {
  int addColumn(String name, RexNode column, RelDataTypeFactory typeFactory);

  RelDataType getRowType();

  int getNumColumns();

  int getNumSelects();

  String getNameId();

  boolean isLocked();
}
