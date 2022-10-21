package ai.datasqrl.plan.calcite.rules;

import ai.datasqrl.function.SqrlFunction;
import ai.datasqrl.function.TimestampPreservingFunction;
import ai.datasqrl.function.builtin.time.StdTimeLibraryImpl;
import ai.datasqrl.physical.EngineCapability;
import ai.datasqrl.physical.pipeline.ExecutionStage;
import ai.datasqrl.plan.calcite.util.SqrlRexUtil;
import lombok.Value;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;

import java.util.*;

@Value
public class ExecutionAnalysis {

    ExecutionStage stage;

    public static ExecutionAnalysis ofScan(ExecutionStage scan) {
        return new ExecutionAnalysis(scan);
    }

    public static ExecutionAnalysis start(ExecutionStage start) {
        return new ExecutionAnalysis(start);
    }

    public boolean isMaterialize(ExecutionAnalysis from) {
        return stage.isMaterialize(from.stage);
    }

    public ExecutionAnalysis combine(ExecutionAnalysis other) {
        if (stage.equals(other.stage)) return this;
        Optional<ExecutionStage> next = stage.nextStage();
        if (next.filter(s -> s.equals(other.stage)).isPresent()) return other;
        Optional<ExecutionStage> otherNext = other.stage.nextStage();
        if (otherNext.filter(s -> s.equals(stage)).isPresent()) return this;
        if (otherNext.isPresent() && next.isPresent() && next.get().equals(otherNext.get())) {
            return new ExecutionAnalysis(next.get());
        }
        throw new IllegalStateException("Could not combine stages: " + stage + " vs " + other.stage);
    }

    public ExecutionAnalysis require(EngineCapability... requiredCapabilities) {
        return require(Arrays.asList(requiredCapabilities));
    }

    public ExecutionAnalysis require(Collection<EngineCapability> requiredCapabilities) {
        if (stage.supportsAll(requiredCapabilities)) return this;

        Optional<ExecutionStage> nextStage = stage.nextStage();
        while (nextStage.isPresent()) {
            if (nextStage.get().supportsAll(requiredCapabilities)) return new ExecutionAnalysis(nextStage.get());
            else nextStage = nextStage.get().nextStage();
        }
        throw new IllegalStateException("Could not find stage that supports capabilities: " + requiredCapabilities);
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
            Optional<SqrlFunction> sqrlFunction = SqrlRexUtil.unwrapSqrlFunction(call.getOperator());
            if (sqrlFunction.filter(func -> func instanceof StdTimeLibraryImpl.NOW).isPresent()) {
                capabilities.add(EngineCapability.NOW);
            } else if (sqrlFunction.filter(func -> func instanceof TimestampPreservingFunction).isPresent()) {
                capabilities.add(EngineCapability.EXTENDED_FUNCTIONS);
            }
            return super.visitCall(call);
        }

    }

}
