package ai.datasqrl.physical.pipeline;

import ai.datasqrl.physical.EngineCapability;
import ai.datasqrl.physical.EnginePhysicalPlan;
import ai.datasqrl.physical.ExecutionEngine;
import ai.datasqrl.physical.ExecutionResult;
import ai.datasqrl.plan.global.OptimizedDAG;
import lombok.Value;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;
import java.util.Optional;

@Value
public class EngineStage implements ExecutionStage {

    ExecutionEngine engine;
    Optional<ExecutionStage> next;

    @Override
    public String getName() {
        return engine.getName();
    }

    @Override
    public boolean supports(EngineCapability capability) {
        return engine.supports(capability);
    }

    @Override
    public Optional<ExecutionStage> nextStage() {
        return next;
    }

    @Override
    public ExecutionResult execute(EnginePhysicalPlan plan) {
        return engine.execute(plan);
    }

    @Override
    public EnginePhysicalPlan plan(OptimizedDAG.StagePlan plan, List<OptimizedDAG.StageSink> inputs, RelBuilder relBuilder) {
        return engine.plan(plan,inputs,relBuilder);
    }
}
