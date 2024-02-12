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
public class Relationship implements SqrlTableMacro {
  private final Name name;
  private final NamePath fullPath;
  private final NamePath absolutePath;
  private final JoinType joinType;
  private final Multiplicity multiplicity;

  private final List<FunctionParameter> parameters;
  private final Supplier<RelNode> viewTransform;

  public Relationship(Name name, NamePath fullPath, NamePath absolutePath, JoinType joinType,
      Multiplicity multiplicity, List<FunctionParameter> parameters,
      Supplier<RelNode> viewTransform) {
    this.name = name;
    this.fullPath = fullPath;
    this.absolutePath = absolutePath;
    this.joinType = joinType;
    this.multiplicity = multiplicity;
    this.parameters = parameters;
    this.viewTransform = viewTransform;
  }

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