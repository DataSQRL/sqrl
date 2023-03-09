package com.datasqrl.plan.local.generate;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.name.NameCanonicalizer;
import com.datasqrl.plan.calcite.rules.AnnotatedLP;
import com.datasqrl.plan.calcite.table.CalciteTableFactory;
import java.util.function.Function;
import org.apache.calcite.rel.RelNode;

public class DistinctStatementResolver extends AbstractQueryStatementResolver {

  protected DistinctStatementResolver(ErrorCollector errors,
      NameCanonicalizer nameCanonicalizer, SqrlQueryPlanner planner, CalciteTableFactory tableFactory) {
    super(errors, nameCanonicalizer, planner, tableFactory);
  }

  @Override
  public Function<AnnotatedLP, AnnotatedLP> getPostProcessor(Namespace ns, RelNode relNode) {
    return (prel) ->
        postProcessAnnotatedLP(planner.createRelBuilder(), prel, prel.relNode.getRowType().getFieldNames());
  }
}
