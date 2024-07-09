package com.datasqrl.calcite.function;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship.JoinType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.function.Supplier;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
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

  String getDisplayName();

  Multiplicity getMultiplicity();

  JoinType getJoinType();

  boolean isTest();

  SqrlTableMacro rename(Name destName);
}
