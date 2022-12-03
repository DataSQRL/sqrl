package com.datasqrl.physical.pipeline;

import com.datasqrl.physical.ExecutionEngine;

import java.util.Collection;
import java.util.Optional;

public interface ExecutionPipeline {

    Collection<ExecutionStage> getStages();

    /**
     * We currently make the simplifying assumption that an {@link ExecutionPipeline} contains
     * at most one stage for any {@link ExecutionEngine.Type}. This is not true in full generality
     * and requires significant changes to the DAGPlanner and import mechanism to support.
     *
     * @param type
     * @return the stage for a given {@link ExecutionEngine.Type}.
     */
    Optional<ExecutionStage> getStage(ExecutionEngine.Type type);

}
