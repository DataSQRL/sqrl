/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import com.datasqrl.canonicalizer.Name;
import com.google.common.base.Preconditions;
import lombok.Getter;

@Getter
public class Relationship extends Field {

  private final SQRLTable fromTable;
  private final SQRLTable toTable;
  private final JoinType joinType;
  private final Multiplicity multiplicity;

  public Relationship(Name name, int version, SQRLTable fromTable, SQRLTable toTable,
      JoinType joinType,
      Multiplicity multiplicity) {
    super(name, version);
    this.fromTable = fromTable;
    this.toTable = toTable;
    Preconditions.checkNotNull(toTable);
    this.joinType = joinType;
    this.multiplicity = multiplicity;
  }

  @Override
  public String toString() {
    return name.getCanonical() + " : " + fromTable.getName() + " -> " + toTable.getName()
        + " [" + joinType + "," + multiplicity + "]";
  }

  @Override
  public FieldKind getKind() {
    return FieldKind.RELATIONSHIP;
  }

  public enum JoinType {
    PARENT, CHILD, JOIN
  }

  public <R, C> R accept(FieldVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}