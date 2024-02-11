package com.datasqrl.actions;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.plan.queries.APIQuery;
import com.google.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_=@Inject)
public class CreateDatabaseQueries {

  private final ExecutionPipeline pipeline;
  private final SqrlFramework framework;
  private final APIConnectorManager apiConnectorManager;

  public void run() {
    if (pipeline.getStage(Type.DATABASE).isPresent() &&
        pipeline.getStage(Type.SERVER).isEmpty()) {
      AtomicInteger i = new AtomicInteger();
      framework.getSchema().getTableFunctions()
          .forEach(t->apiConnectorManager.addQuery(new APIQuery(
              "query" + i.incrementAndGet(),
              framework.getQueryPlanner().expandMacros(t.getViewTransform().get()))));
//              t.getParameters().stream()
//                  .map(p->(SqrlFunctionParameter)p)
//                  .collect(Collectors.toList()),
//              t.getAbsolutePath()))
//          );

    }
  }
}
