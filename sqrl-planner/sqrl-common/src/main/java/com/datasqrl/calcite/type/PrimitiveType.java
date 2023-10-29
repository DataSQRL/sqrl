package com.datasqrl.calcite.type;


import com.datasqrl.calcite.Dialect;

public interface PrimitiveType extends SqrlType {

  String getPhysicalTypeName(Dialect dialect);

}
