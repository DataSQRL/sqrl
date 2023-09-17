package com.datasqrl.calcite;

import com.datasqrl.schema.SQRLTable;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;

public interface ModifiableTable {
  void addColumn(String name, RexNode column, RelDataTypeFactory typeFactory);

  SQRLTable getSqrlTable();

  String getNameId();

}
