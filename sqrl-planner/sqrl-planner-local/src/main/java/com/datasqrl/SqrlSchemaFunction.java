package com.datasqrl;

import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlOperator;

import java.util.List;

public class SqrlSchemaFunction implements Function {
  @Override
  public List<FunctionParameter> getParameters() {
    return null;
  }

  public SqlOperator getOperator() {
    return null;
  }
}
