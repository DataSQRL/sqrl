package ai.datasqrl.plan.local.generate;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.environment.ImportManager;
import ai.datasqrl.physical.pipeline.EngineStage;
import ai.datasqrl.physical.pipeline.ExecutionPipeline;
import ai.datasqrl.plan.calcite.Planner;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class Session {
  ErrorCollector errors;
  ImportManager importManager;
  Planner planner;
  ExecutionPipeline pipeline;

  public Session(ErrorCollector errors, ImportManager importManager, Planner planner) {
    this(errors,importManager,planner, EngineStage.streamDatabasePipeline());
  }
}