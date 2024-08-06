package com.datasqrl.schema;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.schema.Relationship.JoinType;
import java.util.List;
import java.util.function.Supplier;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.Table;

@Getter
public class RootSqrlTable implements SqrlTableMacro {
  private final Name name;
  private final Table internalTable;
  private final List<FunctionParameter> parameters;
  private final Supplier<RelNode> viewTransform;
  private final NamePath fullPath;
  private final boolean isTest;

  public RootSqrlTable(Name name, Table internalTable, List<FunctionParameter> parameters,
      Supplier<RelNode> viewTransform, boolean isTest) {
    this.name = name;
    this.internalTable = internalTable;
    this.parameters = parameters;
    this.viewTransform = viewTransform;
    this.fullPath = NamePath.of(name);
    this.isTest = isTest;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory, List<Object> list) {
    return viewTransform.get().getRowType();
  }

  @Override
  public Supplier<RelNode> getViewTransform() {
    return viewTransform;
  }

  @Override
  public RelDataType getRowType() {
    return getRowType(null, null);
  }

  @Override
  public NamePath getAbsolutePath() {
    return fullPath;
  }

  @Override
  public String getDisplayName() {
    return getName().getDisplay();
  }

  @Override
  public Multiplicity getMultiplicity() {
    return Multiplicity.MANY;
  }

  @Override
  public JoinType getJoinType() {
    return JoinType.NONE;
  }
}
