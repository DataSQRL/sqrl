package com.datasqrl.calcite.type;

import org.apache.calcite.sql.type.SqlTypeName;

public class Vector extends BridgingFlinkType {

  public Vector(TypeFactory typeFactory) {
    super(typeFactory.createType(FlinkVectorType.class),
        null, null, null, null);
  }

  @Override
  public boolean isStruct() {
    return false;
  }

  @Override
  public SqlTypeName getSqlTypeName() {
    return SqlTypeName.ANY;
  }
}
