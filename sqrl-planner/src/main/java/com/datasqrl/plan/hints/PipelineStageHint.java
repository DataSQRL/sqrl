package com.datasqrl.plan.hints;

import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import java.util.Optional;
import lombok.Value;

@Value
public class PipelineStageHint implements OptimizerHint {

  public static final String HINT_NAME = "exec";

  String stageName;

  public ExecutionStage getStage(ExecutionPipeline pipeline, ErrorCollector errors) {
    Optional<ExecutionStage> stage = pipeline.getStage(stageName)
        .or(() -> pipeline.getStageByType(stageName));
    errors.checkFatal(stage.isPresent(),
        "Could not find execution stage [%s] specified in optimizer hint", stageName);
    return stage.get();
  }

}
