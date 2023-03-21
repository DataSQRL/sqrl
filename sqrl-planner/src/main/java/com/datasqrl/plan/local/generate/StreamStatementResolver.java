package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.name.NameCanonicalizer;
import com.datasqrl.plan.calcite.rel.LogicalStream;
import com.datasqrl.plan.calcite.table.CalciteTableFactory;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.Assignment;
import org.apache.calcite.sql.StreamAssignment;

public class StreamStatementResolver extends QueryStatementResolver {


  protected StreamStatementResolver(ErrorCollector errors,
      NameCanonicalizer nameCanonicalizer, SqrlQueryPlanner planner, CalciteTableFactory tableFactory) {
    super(errors, nameCanonicalizer, planner, tableFactory);
  }

  @Override
  protected RelNode preprocessRelNode(RelNode relNode, Assignment statement) {
    Preconditions.checkArgument(statement instanceof StreamAssignment);
    return LogicalStream.create(relNode, ((StreamAssignment)statement).getType());
  }

}
