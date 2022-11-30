package ai.datasqrl.plan.local.generate;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.physical.pipeline.ExecutionPipeline;
import ai.datasqrl.plan.calcite.Planner;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class Session {

  ErrorCollector errors;
  Planner planner;
  ExecutionPipeline pipeline;

}