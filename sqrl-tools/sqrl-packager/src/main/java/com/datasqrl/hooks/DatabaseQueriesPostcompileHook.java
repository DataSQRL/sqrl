package com.datasqrl.hooks;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.inject.AutoBind;
import com.datasqrl.injector.PostcompileHook;
import com.datasqrl.plan.queries.APIQuery;
import com.google.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;

@AutoBind(PostcompileHook.class)
public class DatabaseQueriesPostcompileHook implements PostcompileHook {

  private final ExecutionPipeline pipeline;
  private final SqrlFramework framework;
  private final APIConnectorManager apiConnectorManager;

  @Inject
  public DatabaseQueriesPostcompileHook(ExecutionPipeline pipeline,
      SqrlFramework framework,
      APIConnectorManager apiConnectorManager) {

    this.pipeline = pipeline;
    this.framework = framework;
    this.apiConnectorManager = apiConnectorManager;
  }

  @Override
  public void runHook() {
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
