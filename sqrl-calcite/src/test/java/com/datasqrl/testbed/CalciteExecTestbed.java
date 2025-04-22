package com.datasqrl.testbed;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.Pair;

class CalciteExecTestbed {

  // Executes a trivial rex
  public EnumerableRel trivialRex(RelRoot root, EnumerableRel enumerable) {
    final List<RexNode> projects = new ArrayList<>();
    final var rexBuilder = enumerable.getCluster().getRexBuilder();
    for (int field : Pair.left(root.fields)) {
      projects.add(rexBuilder.makeInputRef(enumerable, field));
    }
    var program = RexProgram.create(enumerable.getRowType(),
        projects, null, root.validatedRowType, rexBuilder);
    enumerable = EnumerableCalc.create(enumerable, program);
    return enumerable;
  }
}