package com.datasqrl.plan.local.generate;

import com.datasqrl.plan.rules.LPAnalysis;
import com.datasqrl.plan.table.ScriptRelationalTable;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;


public class ComputeTableFunction extends TableFunctionBase {

  public ComputeTableFunction(LPAnalysis lpAnalysis, List<FunctionParameter> params, ScriptRelationalTable q) {
    super(lpAnalysis, params, q);
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
