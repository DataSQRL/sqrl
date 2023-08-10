package com.datasqrl.plan.local.generate;

import com.datasqrl.plan.rules.LPAnalysis;
import com.datasqrl.plan.table.ScriptRelationalTable;
import com.datasqrl.schema.SQRLTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;

import java.lang.reflect.Type;
import java.util.List;

public class AccessTableFunction extends TableFunctionBase {


  public AccessTableFunction(LPAnalysis lpAnalysis, List<FunctionParameter> params, SQRLTable table, ScriptRelationalTable q) {
    super(lpAnalysis, params, table, q);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory, List<Object> list) {
    return lpAnalysis.getOriginalRelnode()
        .getRowType();
  }

  @Override
  public Type getElementType(List<Object> list) {
    return Object.class;
  }

  @Override
  public List<FunctionParameter> getParameters() {
    return params;
  }
}
