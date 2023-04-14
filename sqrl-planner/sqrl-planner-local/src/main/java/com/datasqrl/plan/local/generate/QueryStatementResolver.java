package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.plan.table.CalciteTableFactory;

public class QueryStatementResolver extends AbstractQueryStatementResolver {

  protected QueryStatementResolver(ErrorCollector errors,
      NameCanonicalizer nameCanonicalizer, SqrlQueryPlanner planner, CalciteTableFactory tableFactory) {
    super(errors, nameCanonicalizer, planner, tableFactory);
  }

}
