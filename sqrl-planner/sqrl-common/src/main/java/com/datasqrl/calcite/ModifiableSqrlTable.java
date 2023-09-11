package com.datasqrl.calcite;

import com.datasqrl.schema.SQRLTable;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;

public interface ModifiableSqrlTable extends Table {
  void addColumn(String name, RexNode column, RelDataTypeFactory typeFactory);

  SQRLTable getSqrlTable();

  String getName();

}
