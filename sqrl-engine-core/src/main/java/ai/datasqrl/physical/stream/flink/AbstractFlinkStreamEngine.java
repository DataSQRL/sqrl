package ai.datasqrl.physical.stream.flink;

import ai.datasqrl.physical.EngineCapability;
import ai.datasqrl.physical.EnginePhysicalPlan;
import ai.datasqrl.physical.ExecutionEngine;
import ai.datasqrl.physical.ExecutionResult;
import ai.datasqrl.physical.stream.flink.plan.FlinkPhysicalPlanner;
import ai.datasqrl.physical.stream.flink.plan.FlinkStreamPhysicalPlan;
import ai.datasqrl.plan.global.OptimizedDAG;
import com.google.common.base.Preconditions;
import org.apache.calcite.tools.RelBuilder;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;

import java.util.EnumSet;
import java.util.List;

import static ai.datasqrl.physical.EngineCapability.*;

public abstract class AbstractFlinkStreamEngine extends ExecutionEngine.Base implements FlinkStreamEngine {

    public static final EnumSet<EngineCapability> FLINK_CAPABILITIES = EnumSet.of(DENORMALIZE, TEMPORAL_JOIN,
            TIME_WINDOW_AGGREGATION, EXTENDED_FUNCTIONS, CUSTOM_FUNCTIONS);

    final FlinkEngineConfiguration config;

    public AbstractFlinkStreamEngine(FlinkEngineConfiguration config) {
        super(FlinkEngineConfiguration.ENGINE_NAME, Type.STREAM, FLINK_CAPABILITIES);
        this.config = config;
    }

    @Override
    public ExecutionResult execute(EnginePhysicalPlan plan) {
        Preconditions.checkArgument(plan instanceof FlinkStreamPhysicalPlan);
        FlinkStreamPhysicalPlan flinkPlan = (FlinkStreamPhysicalPlan)plan;
        StatementSet statementSet = flinkPlan.getStatementSet();
        TableResult rslt = statementSet.execute();
        rslt.print(); //todo: this just forces print to wait for the async
        return new ExecutionResult.Message(rslt.getJobClient().get()
                .getJobID().toString());
    }

    @Override
    public FlinkStreamPhysicalPlan plan(OptimizedDAG.StagePlan plan, List<OptimizedDAG.StageSink> inputs, RelBuilder relBuilder) {
        Preconditions.checkArgument(inputs.isEmpty());
        FlinkStreamPhysicalPlan streamPlan = new FlinkPhysicalPlanner(this).createStreamGraph(plan.getQueries());
        return streamPlan;
    }




}
