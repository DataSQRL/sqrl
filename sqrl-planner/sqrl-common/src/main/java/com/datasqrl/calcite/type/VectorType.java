package com.datasqrl.calcite.type;

import com.datasqrl.calcite.Dialect;
import java.util.Map;
import org.apache.calcite.sql.type.SqlTypeName;

public class VectorType extends BridgingFlinkType implements PrimitiveType {

  public VectorType(TypeFactory typeFactory) {
    super(typeFactory.createType(FlinkVectorType.class),
        null,
        Map.of(Dialect.POSTGRES, "VECTOR"));
  }

  @Override
  public boolean isStruct() {
    return false;
  }

  @Override
  public SqlTypeName getSqlTypeName() {
    return SqlTypeName.STRUCTURED;
  }
}
