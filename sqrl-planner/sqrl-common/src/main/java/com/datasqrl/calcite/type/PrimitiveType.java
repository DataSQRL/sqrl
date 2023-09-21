package com.datasqrl.calcite.type;


import com.datasqrl.calcite.Dialect;

public interface PrimitiveType {

  String getPhysicalTypeName(Dialect dialect);

}
