package com.datasqrl.testbed;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.Pair;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class CalciteExecTestbed {

  // Executes a trivial rex
  public EnumerableRel trivialRex(RelRoot root, EnumerableRel enumerable) {
    final List<RexNode> projects = new ArrayList<>();
    final RexBuilder rexBuilder = enumerable.getCluster().getRexBuilder();
    for (int field : Pair.left(root.fields)) {
      projects.add(rexBuilder.makeInputRef(enumerable, field));
    }
    RexProgram program = RexProgram.create(enumerable.getRowType(),
        projects, null, root.validatedRowType, rexBuilder);
    enumerable = EnumerableCalc.create(enumerable, program);
    return enumerable;
  }
}