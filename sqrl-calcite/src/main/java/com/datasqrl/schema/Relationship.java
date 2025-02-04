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
  private final NamePath fullPath; //todo: remove, we just have the absolutepath now
  private final NamePath absolutePath; //todo: simplify the path to just type.field since we no longer do path walking
  private final JoinType joinType;  //todo: remove
  private final Multiplicity multiplicity; //derived from limit

  private final List<FunctionParameter> parameters;
  private final Supplier<RelNode> viewTransform;

  private final boolean isTest;

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory, List<? extends Object> list) {
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