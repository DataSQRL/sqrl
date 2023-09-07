package com.datasqrl.util;

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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CalciteExecUtil {
  @SneakyThrows
  public void executeWithCalciteConnection(String sql) {
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");

    // Connect to Calcite
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    //tables

    // Create a statement and execute a query
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(sql);

    // Process the result set
    while (resultSet.next()) {
      System.out.println(resultSet.getString(1));
    }

    resultSet.close();
    statement.close();
    connection.close();
  }

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
