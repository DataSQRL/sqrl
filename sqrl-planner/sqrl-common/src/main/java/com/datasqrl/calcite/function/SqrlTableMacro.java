package com.datasqrl.calcite.function;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.schema.SQRLTable;
import java.lang.reflect.Type;
import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.TableFunction;

public interface SqrlTableMacro extends TableFunction {

  @Override
  default Type getElementType(List<Object> list) {
    return Object[].class;
  }

  Supplier<RelNode> getViewTransform();

  RelDataType getRowType();

  NamePath getPath();
}
