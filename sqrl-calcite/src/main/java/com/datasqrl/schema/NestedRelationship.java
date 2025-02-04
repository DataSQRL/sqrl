package com.datasqrl.schema;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import java.util.List;
import java.util.function.Supplier;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;

/**
 * This can go entirely, we don't have nested relationships anymore
 */
@Getter
public class NestedRelationship extends Relationship {

  private final RelDataType rowType;
  private final int[] localPKs;

  public NestedRelationship(Name name,
      NamePath fullPath, NamePath absolutePath,
      Multiplicity multiplicity,
      List<FunctionParameter> parameters, RelDataType rowType, int[] localPKs, boolean isTest) {
    super(name, fullPath, absolutePath, JoinType.CHILD, multiplicity, parameters, null,
        isTest);
    this.rowType = rowType;
    this.localPKs = localPKs;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory, List<? extends Object> list) {
    return getRowType();
  }

  @Override
  public RelDataType getRowType() {
    return rowType;
  }

  @Override
  public Supplier<RelNode> getViewTransform() {
    throw new UnsupportedOperationException();
  }
}
