package ai.datasqrl.physical.pipeline;

import ai.datasqrl.physical.EngineCapability;
import ai.datasqrl.physical.EnginePhysicalPlan;
import ai.datasqrl.physical.ExecutionEngine;
import ai.datasqrl.physical.ExecutionResult;
import ai.datasqrl.plan.global.OptimizedDAG;
import org.apache.calcite.tools.RelBuilder;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface ExecutionStage {

    default String getName() {
        return getEngine().getName();
    }

    default boolean supportsAll(Collection<EngineCapability> capabilities) {
        return capabilities.stream().allMatch(this::supports);
    }

    boolean supports(EngineCapability capability);

    ExecutionEngine getEngine();

    default boolean isWrite() {
        return getEngine().getType().isWrite();
    }

    default boolean isRead() {
        return getEngine().getType().isRead();
    }

    /**
     *
     * @param from
     * @return Whether going from the given stage to this one crosses the materialization boundary
     */
    default boolean isMaterialize(ExecutionStage from) {
        return from.isWrite() && isRead();
    }

    /**
     * We currently make the simplifying assumption that an {@link ExecutionPipeline} has a tree structure.
     * To generalize this to a DAG structure (e.g. to support multiple database engines) we need to make
     * significant changes to the LPConverter and DAGPlanner. See also {@link ExecutionPipeline#getStage(ExecutionEngine.Type)}.
     *
     * @return Next execution stage in this pipeline
     */
    Optional<ExecutionStage> nextStage();

    ExecutionResult execute(EnginePhysicalPlan plan);

    EnginePhysicalPlan plan(OptimizedDAG.StagePlan plan, List<OptimizedDAG.StageSink> inputs, RelBuilder relBuilder);


}
