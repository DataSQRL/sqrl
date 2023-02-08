package com.datasqrl.plan.local.generate;

import com.datasqrl.plan.calcite.rules.AnnotatedLP;
import com.datasqrl.plan.local.generate.SqrlStatementVisitor.SystemContext;
import java.util.function.Function;
import org.apache.calcite.rel.RelNode;

public class StreamStatementResolver extends AbstractQueryStatementResolver {

  public StreamStatementResolver(SystemContext systemContext) {
    super(systemContext);
  }

  @Override
  public Function<AnnotatedLP, AnnotatedLP> getPostProcessor(FlinkNamespace ns, RelNode relNode) {
    return  (prel) ->
        postProcessStreamAnnotatedLP(ns.createRelBuilder(), prel, relNode.getRowType().getFieldNames());
  }
}
