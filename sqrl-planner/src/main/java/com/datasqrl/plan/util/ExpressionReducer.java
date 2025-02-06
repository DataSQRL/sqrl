/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.table.api.TableConfig;

/**
 * Thin wrapper around Flink's {@org.apache.flink.table.planner.codegen.ExpressionReducer}
 */
public class ExpressionReducer {

  private final org.apache.flink.table.planner.codegen.ExpressionReducer reducer =
      new org.apache.flink.table.planner.codegen.ExpressionReducer(TableConfig.getDefault(), ClassLoader.getSystemClassLoader(), true);

  List<RexNode> reduce(RexBuilder rexBuilder, List<RexNode> original) {
    var reduced = new ArrayList<RexNode>();
    reducer.reduce(rexBuilder, original, reduced);
    assert reduced.size() == original.size();
    return reduced;
  }

  long[] reduce2Long(RexBuilder rexBuilder, List<RexNode> original) {
    var longs = new long[original.size()];
    var reduced = reduce(rexBuilder, original);
    for (var i = 0; i < reduced.size(); i++) {
      var reduce = reduced.get(i);
      if (!(reduce instanceof RexLiteral)) {
        throw new IllegalArgumentException(
            "Expression [%s] could not be reduced to literal, got: %s".formatted(
                original.get(i), reduce));
      }
      Object value = ((RexLiteral) reduce).getValue2();
      if (!(value instanceof Number)) {
        throw new IllegalArgumentException(
            "Value of reduced literal [%s] is not a number: %s".formatted(reduce, value));
      }
      longs[i] = ((Number) value).longValue();
    }
    return longs;
  }

}
