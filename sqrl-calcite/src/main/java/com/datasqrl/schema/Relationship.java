/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import java.util.List;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;

@Getter
@AllArgsConstructor
public class Relationship implements SqrlTableMacro {
  private final Name name;
  private final NamePath fullPath;
  private final NamePath absolutePath;
  private final JoinType joinType;
  private final Multiplicity multiplicity;

  private final List<FunctionParameter> parameters;
  private final Supplier<RelNode> viewTransform;

  private final boolean isTest;

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory, List<Object> list) {
    return getRowType();
  }

  @Override
  public RelDataType getRowType() {
    return viewTransform.get().getRowType();
  }

  public String getDisplayName() {
    return getFullPath().getDisplay();
  }

  public enum JoinType {
    NONE, PARENT, CHILD, JOIN
  }
}