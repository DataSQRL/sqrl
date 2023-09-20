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
public class Relationship extends Field implements SqrlTableMacro {
  private final NamePath path;

  private final SQRLTable fromTable;
  private final JoinType joinType;
  private final Multiplicity multiplicity;

  private final List<SQRLTable> isA;
  private final List<FunctionParameter> parameters;
  private final Supplier<RelNode> viewTransform;

  public Relationship(Name name, NamePath path, int version, SQRLTable fromTable,
      JoinType joinType, Multiplicity multiplicity, List<SQRLTable> isA, List<FunctionParameter> parameters,
      Supplier<RelNode> viewTransform) {
    super(name, version);
    this.fromTable = fromTable;
    this.isA = isA;
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

  public SQRLTable getToTable() {
    return isA.get(0);
  }

  public SQRLTable getSqrlTable() {
    return getToTable();
  }

  public String getNameId() {
    return getToTable().getNameId();
  }

  public enum JoinType {
    PARENT, CHILD, JOIN
  }

  public <R, C> R accept(FieldVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}