package com.datasqrl.schema;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.google.common.base.Supplier;
import java.lang.reflect.Type;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFunction;

@Getter
public class RootSqrlTable extends SQRLTable implements SqrlTableMacro {
  private final Name name;
  private final List<FunctionParameter> parameters;

  public RootSqrlTable(Name name, Table internalTable, List<String> isTypeOf,
      List<FunctionParameter> parameters) {
    super(name.toNamePath(), internalTable, isTypeOf);
    this.name = name;
    this.parameters = parameters;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory, List<Object> list) {
    return relOptTable.getRowType(null);
  }

  @Override
  public Type getElementType(List<Object> list) {
    return Object.class;
  }

  @Override
  public Supplier<RelNode> getViewTransform() {
    return ()->null;
  }
}
