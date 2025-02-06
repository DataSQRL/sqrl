package com.datasqrl.actions;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.sql.DatabaseQueryFactory;
import com.google.inject.Inject;

import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_=@Inject)
public class CreateDatabaseQueries {

  private final ExecutionPipeline pipeline;
  private final SqrlFramework framework;
  private final DatabaseQueryFactory databaseQueryFactory;

  public void run() {
    if (pipeline.getStage(Type.DATABASE).isPresent() &&
        pipeline.getStage(Type.SERVER).isEmpty()) {
      var apiQueries = databaseQueryFactory.generateQueries(framework.getSchema());
      apiQueries.forEach(q->framework.getSchema().getQueries().add(q));
    }
  }
}
