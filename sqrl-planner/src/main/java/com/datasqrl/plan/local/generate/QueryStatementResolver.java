package com.datasqrl.plan.local.generate;

import com.datasqrl.plan.calcite.rules.AnnotatedLP;
import com.datasqrl.plan.local.generate.SqrlStatementVisitor.SystemContext;
import java.util.function.Function;
import org.apache.calcite.rel.RelNode;

public class QueryStatementResolver extends AbstractQueryStatementResolver {

  public QueryStatementResolver(SystemContext systemContext) {
    super(systemContext);
  }

  @Override
  public Function<AnnotatedLP, AnnotatedLP> getPostProcessor(Namespace ns, RelNode relNode) {
    return (prel) ->
        postProcessAnnotatedLP(ns.createRelBuilder(), prel, relNode.getRowType().getFieldNames());
  }
}
