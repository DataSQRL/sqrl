/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.rules;

import org.apache.calcite.rel.LogicalStream;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.logical.*;

/**
 * A {@link RelShuttle} that throws exceptions for all logical operators that cannot occur in an
 * SQRL logical plan.
 */
public interface SqrlRelShuttle extends RelShuttle {



    /*
    ====== Rel Nodes are not yet supported =====
     */

  @Override
  default RelNode visit(LogicalIntersect logicalIntersect) {
    throw new UnsupportedOperationException("Not yet supported.");
  }

  @Override
  default RelNode visit(LogicalMinus logicalMinus) {
    throw new UnsupportedOperationException("Not yet supported.");
  }

  @Override
  default RelNode visit(LogicalValues logicalValues) {
    throw new UnsupportedOperationException("Not yet supported.");
  }

    /*
    ====== Rel Nodes that do not occur in SQRL =====
     */

  @Override
  default RelNode visit(RelNode relNode) {
    if (relNode instanceof LogicalStream) {
      return visit((LogicalStream) relNode);
    }
    throw new UnsupportedOperationException("Unexpected rel node: " + relNode);
  }

  @Override
  default RelNode visit(TableFunctionScan tableFunctionScan) {
    return visit((RelNode) tableFunctionScan);
  }

  @Override
  default RelNode visit(LogicalCorrelate logicalCorrelate) {
    return visit((RelNode) logicalCorrelate);
  }

  @Override
  default RelNode visit(LogicalCalc logicalCalc) {
    return visit((RelNode) logicalCalc);
  }

  @Override
  default RelNode visit(LogicalExchange logicalExchange) {
    return visit((RelNode) logicalExchange);
  }

  @Override
  default RelNode visit(LogicalTableModify logicalTableModify) {
    return visit((RelNode) logicalTableModify);
  }

  default RelNode visit(LogicalStream stream) {
    throw new RuntimeException("to implement");
  }

  @Override
  default RelNode visit(LogicalMatch logicalMatch) {
    return visit((RelNode) logicalMatch);
  }

}
