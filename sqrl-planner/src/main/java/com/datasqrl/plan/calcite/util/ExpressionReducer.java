/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.calcite.util;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.table.api.TableConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Thin wrapper around Flink's {@org.apache.flink.table.planner.codegen.ExpressionReducer}
 */
public class ExpressionReducer {

  private final org.apache.flink.table.planner.codegen.ExpressionReducer reducer =
      new org.apache.flink.table.planner.codegen.ExpressionReducer(TableConfig.getDefault(), true);

  List<RexNode> reduce(RexBuilder rexBuilder, List<RexNode> original) {
    ArrayList<RexNode> reduced = new ArrayList<>();
    reducer.reduce(rexBuilder, original, reduced);
    assert reduced.size() == original.size();
    return reduced;
  }

  long[] reduce2Long(RexBuilder rexBuilder, List<RexNode> original) {
    long[] longs = new long[original.size()];
    List<RexNode> reduced = reduce(rexBuilder, original);
    for (int i = 0; i < reduced.size(); i++) {
      RexNode reduce = reduced.get(i);
      if (!(reduce instanceof RexLiteral)) {
        throw new IllegalArgumentException(
            String.format("Expression [%s] could not be reduced to literal, got: %s",
                original.get(i), reduce));
      }
      Object value = ((RexLiteral) reduce).getValue2();
      if (!(value instanceof Number)) {
        throw new IllegalArgumentException(
            String.format("Value of reduced literal [%s] is not a number: %s", reduce, value));
      }
      longs[i] = ((Number) value).longValue();
    }
    return longs;
  }

}
