package com.datasqrl.calcite.type;


import org.apache.calcite.sql.SqlFunction;

public interface PrimitiveTypeAlias {

  Class<?> getConversionClass();
  SqlFunction getDowncastFunction();
  SqlFunction getUpcastFunction();

}
