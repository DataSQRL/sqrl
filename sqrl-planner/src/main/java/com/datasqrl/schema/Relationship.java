/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema;

import com.datasqrl.name.Name;
import com.google.common.base.Preconditions;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.SqrlJoinDeclarationSpec;

@Getter
public class Relationship extends Field {

  private final SQRLTable fromTable;
  private final SQRLTable toTable;
  private final JoinType joinType;
  private final Multiplicity multiplicity;
  private final Optional<SqrlJoinDeclarationSpec> join;

  public Relationship(Name name, int version, SQRLTable fromTable, SQRLTable toTable,
      JoinType joinType,
      Multiplicity multiplicity, Optional<SqrlJoinDeclarationSpec> join) {
    super(name, version);
    this.fromTable = fromTable;
    this.toTable = toTable;
    Preconditions.checkNotNull(toTable);
    this.joinType = joinType;
    this.multiplicity = multiplicity;
    this.join = join;
  }

  @Override
  public String toString() {
    return fromTable.getName() + " -> " + toTable.getName()
        + " [" + joinType + "," + multiplicity + "]";
  }

  @Override
  public FieldKind getKind() {
    return FieldKind.RELATIONSHIP;
  }

  public enum JoinType {
    PARENT, CHILD, JOIN
  }

}