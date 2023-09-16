package com.datasqrl.calcite.schema;

import java.lang.reflect.Type;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;

public class SqlPlannerFunction implements TableFunction {



  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory, List<Object> list) {
    return null;
  }

  @Override
  public Type getElementType(List<Object> list) {
    return null;
  }

  @Override
  public List<FunctionParameter> getParameters() {
    return null;
  }
}
