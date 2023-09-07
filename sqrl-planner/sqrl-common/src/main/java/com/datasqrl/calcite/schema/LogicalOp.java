package com.datasqrl.calcite.schema;

import org.apache.calcite.rel.RelNode;

public interface LogicalOp extends RelNode {

    <R, C> R accept(LogicalOpVisitor<R, C> visitor, C context);
  }