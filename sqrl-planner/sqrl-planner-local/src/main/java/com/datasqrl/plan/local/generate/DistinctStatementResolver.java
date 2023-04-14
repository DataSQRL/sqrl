package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.plan.table.CalciteTableFactory;

public class DistinctStatementResolver extends AbstractQueryStatementResolver {

  protected DistinctStatementResolver(ErrorCollector errors,
      NameCanonicalizer nameCanonicalizer, SqrlQueryPlanner planner, CalciteTableFactory tableFactory) {
    super(errors, nameCanonicalizer, planner, tableFactory);
  }

  protected boolean setOriginalFieldnames() {
    return false;
  }
}
