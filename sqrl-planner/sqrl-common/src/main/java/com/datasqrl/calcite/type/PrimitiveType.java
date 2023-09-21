package com.datasqrl.calcite.type;


import com.datasqrl.calcite.Dialect;
import org.apache.calcite.sql.SqlFunction;

public interface PrimitiveType {

  String getPhysicalType(Dialect dialect);

}
