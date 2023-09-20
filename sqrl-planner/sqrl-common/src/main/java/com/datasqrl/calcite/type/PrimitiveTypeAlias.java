package com.datasqrl.calcite.type;


import com.datasqrl.calcite.Dialect;
import org.apache.calcite.sql.SqlFunction;

public interface PrimitiveTypeAlias {

  SqlFunction getDowncastFunction();
  SqlFunction getUpcastFunction();
  Object getPhysicalType(Dialect dialect);

}
