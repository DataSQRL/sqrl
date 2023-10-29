package com.datasqrl.calcite.function;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.function.SqrlFunctionParameter;
import java.lang.reflect.Type;
import java.util.List;
import java.util.function.Supplier;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;

public interface SqrlTableMacro extends TableFunction {

  @Override
  default Type getElementType(List<Object> list) {
    return Object[].class;
  }

  Supplier<RelNode> getViewTransform();

  RelDataType getRowType();

  NamePath getFullPath();
  NamePath getAbsolutePath();

  String getInternalName();
  String getDisplayName();

  @Value
  class Column {
    String name;
    List<SqrlFunctionParameter> parameters;
    boolean computed;
    boolean relationship;
    List<SqrlTableMacro> isA;
    RelDataType type;
  }


}
