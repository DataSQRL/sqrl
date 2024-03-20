/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.rules;

import com.datasqrl.plan.rel.LogicalStream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Snapshot;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalValues;

/**
 * A {@link RelShuttle} that throws exceptions for all logical operators that cannot occur in an
 * SQRL logical plan.
 */
public interface SqrlRelShuttle extends RelShuttle {


  /*
  ====== Rel Nodes with default treatment =====
   */

  @Override
  default RelNode visit(LogicalIntersect logicalIntersect) {
    return visit((RelNode)logicalIntersect);
  }

  @Override
  default RelNode visit(LogicalMinus logicalMinus) {
    return visit((RelNode)logicalMinus);
  }

  @Override
  default RelNode visit(LogicalCalc logicalCalc) {
    return visit((RelNode) logicalCalc);
  }

  default RelNode visit(LogicalExchange logicalExchange) {
    return visit((RelNode) logicalExchange);
  }

  @Override
  default RelNode visit(LogicalMatch logicalMatch) {
    return visit((RelNode) logicalMatch);
  }


  /*
  ====== Rel Nodes that do not occur in SQRL =====
  */

  @Override
  default RelNode visit(LogicalTableModify logicalTableModify) {
    throw new UnsupportedOperationException("Not yet supported.");
  }


}
