package ai.datasqrl.physical.pipeline;

import ai.datasqrl.physical.ExecutionEngine;

import java.util.Collection;
import java.util.Optional;

public interface ExecutionPipeline {

    Collection<ExecutionStage> getStages();

    Optional<ExecutionStage> getStage(ExecutionEngine.Type type);

}
