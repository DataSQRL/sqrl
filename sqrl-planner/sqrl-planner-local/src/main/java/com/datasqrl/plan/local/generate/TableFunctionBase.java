package com.datasqrl.plan.local.generate;

import com.datasqrl.plan.rules.LPAnalysis;
import com.datasqrl.plan.table.ScriptRelationalTable;
import com.datasqrl.schema.SQRLTable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;

import java.lang.reflect.Type;
import java.util.List;

@AllArgsConstructor
@Getter
public abstract class TableFunctionBase implements TableFunction {

  LPAnalysis lpAnalysis;
  List<FunctionParameter> params;
  SQRLTable table;
  ScriptRelationalTable q;

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
