/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import java.util.List;
import java.util.function.Supplier;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;

@Getter
public class Relationship implements SqrlTableMacro {
  private final NamePath path;

  private final Name name;
  private final int version;
  private final NamePath fromTable;
  private final NamePath toTable;
  private final JoinType joinType;
  private final Multiplicity multiplicity;

  private final List<FunctionParameter> parameters;
  private final Supplier<RelNode> viewTransform;

  public Relationship(Name name, NamePath path, int version, NamePath fromTable, NamePath toTable,
      JoinType joinType, Multiplicity multiplicity, List<FunctionParameter> parameters,
      Supplier<RelNode> viewTransform) {
    this.name = name;
    this.version = version;
    this.fromTable = fromTable;
    this.toTable = toTable;
    this.parameters = parameters;
    this.viewTransform = viewTransform;
    this.joinType = joinType;
    this.multiplicity = multiplicity;
    this.path = path;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory, List<Object> list) {
    return viewTransform.get().getRowType();
  }

  @Override
  public RelDataType getRowType() {
    return getRowType(null, null);
  }

  public enum JoinType {
    PARENT, CHILD, JOIN
  }
}