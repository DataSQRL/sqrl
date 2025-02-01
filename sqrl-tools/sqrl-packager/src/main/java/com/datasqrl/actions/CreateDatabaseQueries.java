package com.datasqrl.actions;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.EngineType;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.plan.queries.APIQuery;
import com.datasqrl.sql.DatabaseQueryFactory;
import com.google.inject.Inject;
import java.util.List;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_=@Inject)
public class CreateDatabaseQueries {

  private final ExecutionPipeline pipeline;
  private final SqrlFramework framework;
  private final DatabaseQueryFactory databaseQueryFactory;

  public void run() {
    if (!pipeline.getStagesByType(EngineType.DATABASE).isEmpty() &&
        pipeline.getStageByType(EngineType.SERVER).isEmpty()) {
      List<APIQuery> apiQueries = databaseQueryFactory.generateQueries(framework.getSchema());
      apiQueries.forEach(q->framework.getSchema().getQueries().add(q));
    }
  }
}
