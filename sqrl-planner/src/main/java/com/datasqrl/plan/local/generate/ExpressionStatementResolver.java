package com.datasqrl.plan.local.generate;

import com.datasqrl.plan.local.generate.SqrlStatementVisitor.SystemContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.ExpressionAssignment;
import org.apache.calcite.sql.SqlNode;

public class ExpressionStatementResolver extends AbstractStatementResolver {

  public ExpressionStatementResolver(SystemContext systemContext) {
    super(systemContext);
  }

  public void resolve(ExpressionAssignment statement, FlinkNamespace ns) {
    SqlNode sqlNode = transpile(statement, ns);
    RelNode relNode = plan(sqlNode, ns);

    //Only simple expressions allowed
    addColumn(statement.getNamePath(), ns, relNode, false);
  }
}
