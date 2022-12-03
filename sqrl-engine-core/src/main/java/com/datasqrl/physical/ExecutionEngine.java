package com.datasqrl.physical;

import com.datasqrl.plan.global.OptimizedDAG;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.tools.RelBuilder;

import java.util.EnumSet;
import java.util.List;

/**
 * Describes a physical execution engine and it's capabilities.
 */
public interface ExecutionEngine {

    public enum Type {
        STREAM, DATABASE, SERVER;

        public boolean isWrite() {
            return this==STREAM;
        }
        public boolean isRead() {
            return this==DATABASE || this==SERVER;
        }
    }

    boolean supports(EngineCapability capability);

    Type getType();

    String getName();

    ExecutionResult execute(EnginePhysicalPlan plan);

    EnginePhysicalPlan plan(OptimizedDAG.StagePlan plan, List<OptimizedDAG.StageSink> inputs, RelBuilder relBuilder);

    @AllArgsConstructor
    @Getter
    public static abstract class Base implements ExecutionEngine {

        protected final @NonNull String name;
        protected final @NonNull Type type;
        protected final @NonNull EnumSet<EngineCapability> capabilities;

        @Override
        public boolean supports(EngineCapability capability) {
            return capabilities.contains(capability);
        }

    }

}
