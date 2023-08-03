package com.datasqrl.plan.local.generate;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.plan.rules.LPAnalysis;
import com.datasqrl.plan.table.ScriptTable;
import com.datasqrl.schema.SQRLTable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;

import java.lang.reflect.Type;
import java.util.List;

@AllArgsConstructor
@Getter
public abstract class TableFunctionBase implements TableFunction, ScriptTable {

  Name functionName;
  List<FunctionParameter> params;
  SQRLTable table;

  @Override
  public String getNameId() {
    //TODO: should this include parameters or is a function uniquely defined by it's name alone?
    return functionName.getCanonical();
  }

  public abstract LPAnalysis getAnalyzedLP();

  @Override
  public Name getTableName() {
    return functionName;
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
