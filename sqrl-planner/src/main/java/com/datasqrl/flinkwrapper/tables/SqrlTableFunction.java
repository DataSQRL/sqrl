package com.datasqrl.flinkwrapper.tables;

import com.datasqrl.flinkwrapper.analyzer.RelNodeAnalysis;
import com.datasqrl.flinkwrapper.analyzer.TableAnalysis;
import java.lang.reflect.Type;
import java.util.List;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableFunction;

@AllArgsConstructor
@Getter
public class SqrlTableFunction implements TableFunction {

  private final List<FunctionParameter> parameters;
  private final Supplier<RelNode> viewTransform;
  private final TableAnalysis tableAnalysis;

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory, List<? extends Object> list) {
    return viewTransform.get().getRowType();
  }

  @Override
  public Type getElementType(List<? extends Object> list) {
    return Object[].class;
  }

  public RelDataType getRowType() {
    return getRowType(null, null);
  }

}
