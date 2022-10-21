package ai.datasqrl.physical.pipeline;

import ai.datasqrl.physical.EngineCapability;
import ai.datasqrl.physical.ExecutionEngine;

import java.util.Collection;
import java.util.Optional;

public interface ExecutionStage {

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
        return from.isWrite() && getEngine().getType().isRead();
    }

    /**
     *
     * @return Next execution stage in this pipeline
     */
    Optional<ExecutionStage> nextStage();


}
