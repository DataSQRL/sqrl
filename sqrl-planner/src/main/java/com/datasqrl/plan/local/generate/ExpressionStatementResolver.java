package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.name.NameCanonicalizer;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.ExpressionAssignment;
import org.apache.calcite.sql.SqlNode;

public class ExpressionStatementResolver extends AbstractStatementResolver {


  protected ExpressionStatementResolver(ErrorCollector errors,
      NameCanonicalizer nameCanonicalizer, SqrlQueryPlanner planner) {
    super(errors, nameCanonicalizer, planner);
  }

  public void resolve(ExpressionAssignment statement, Namespace ns) {
    SqlNode sqlNode = transpile(statement, ns);
    RelNode relNode = plan(sqlNode);

    //Only simple expressions allowed
    addColumn(statement.getNamePath(), ns, relNode, false);
  }
}
