package com.datasqrl.calcite.function;

import com.datasqrl.schema.SQRLTable;
import java.lang.reflect.Type;
import java.util.List;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.TableFunction;

public interface SqrlTableMacro extends TableFunction {


  @Override
  default Type getElementType(List<Object> list) {
    return Object[].class;
  }

  Supplier<RelNode> getViewTransform();

  SQRLTable getSqrlTable();
}