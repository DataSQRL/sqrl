package ai.datasqrl.plan.calcite.rules;

import ai.datasqrl.function.SqrlFunction;
import ai.datasqrl.function.TimestampPreservingFunction;
import ai.datasqrl.function.builtin.time.StdTimeLibraryImpl;
import ai.datasqrl.physical.EngineCapability;
import ai.datasqrl.physical.pipeline.ExecutionStage;
import lombok.Value;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.commons.collections.ListUtils;

import java.util.*;

@Value
public class ExecutionAnalysis {

    ExecutionStage stage;
    List<AnnotatedLP> inputs;

    public static ExecutionAnalysis of(AnnotatedLP alp) {
        return new ExecutionAnalysis(alp.getStage(), List.of(alp));
    }

    public static ExecutionAnalysis start(ExecutionStage start) {
        return new ExecutionAnalysis(start, List.of());
    }

    public ExecutionStage getStage() {
        return stage;
    }

    public boolean isMaterialize(ExecutionAnalysis from) {
        return stage.isMaterialize(from.stage);
    }

    public ExecutionAnalysis combine(ExecutionAnalysis other) {
        List<AnnotatedLP> inputs = ListUtils.union(this.inputs,other.inputs);
        ExecutionStage resultStage;
        if (stage.equals(other.stage)) {
            resultStage = stage;
        } else {
            Optional<ExecutionStage> next = stage.nextStage();
            Optional<ExecutionStage> otherNext = other.stage.nextStage();
            if (next.filter(s -> s.equals(other.stage)).isPresent()) {
                resultStage = other.stage;
            } else if (otherNext.filter(s -> s.equals(stage)).isPresent()) {
                resultStage = stage;
            } else if (otherNext.isPresent() && next.isPresent() && next.get().equals(otherNext.get())) {
                resultStage = next.get();
            } else {
                throw ExecutionStageException.StageFinding.of(stage,other.stage).injectInputs(inputs);
            }
        }
        return new ExecutionAnalysis(resultStage, inputs);
    }

    public ExecutionAnalysis require(EngineCapability... requiredCapabilities) {
        return require(Arrays.asList(requiredCapabilities));
    }

    public ExecutionAnalysis require(Collection<EngineCapability> requiredCapabilities) {
        if (stage.supportsAll(requiredCapabilities)) return this;

        Optional<ExecutionStage> nextStage = stage.nextStage();
        while (nextStage.isPresent()) {
            if (nextStage.get().supportsAll(requiredCapabilities)) return new ExecutionAnalysis(nextStage.get(), inputs);
            else nextStage = nextStage.get().nextStage();
        }
        throw ExecutionStageException.StageFinding.of(stage,requiredCapabilities).injectInputs(inputs);
    }

    public ExecutionAnalysis requireRex(Iterable<RexNode> nodes) {
        RexCapabilityAnalysis rexAnalysis = new RexCapabilityAnalysis();
        nodes.forEach(rex -> rex.accept(rexAnalysis));
        return require(rexAnalysis.capabilities);
    }

    public ExecutionAnalysis requireAggregates(Iterable<AggregateCall> aggregates) {
        //TODO: implement once we have non-SQL aggregate functions
        return this;
    }

    public static class RexCapabilityAnalysis extends RexVisitorImpl<Void> {

        private final EnumSet<EngineCapability> capabilities = EnumSet.noneOf(EngineCapability.class);

        public RexCapabilityAnalysis() {
            super(true);
        }

        @Override public Void visitCall(RexCall call) {
            Optional<SqrlFunction> sqrlFunction = SqrlFunction.unwrapSqrlFunction(call.getOperator());
            if (sqrlFunction.filter(func -> func instanceof StdTimeLibraryImpl.NOW).isPresent()) {
                capabilities.add(EngineCapability.NOW);
            } else if (sqrlFunction.filter(func -> func instanceof TimestampPreservingFunction).isPresent()) {
                capabilities.add(EngineCapability.EXTENDED_FUNCTIONS);
            }
            return super.visitCall(call);
        }

    }

}
