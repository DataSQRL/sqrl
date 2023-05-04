package com.datasqrl.engine.server;

import static com.datasqrl.engine.EngineCapability.NO_CAPABILITIES;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.database.relational.JDBCEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.serializer.Deserializer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.tools.RelBuilder;

/**
 * A no-feature java server engine.
 */
public abstract class GenericJavaServerEngine extends ExecutionEngine.Base implements ServerEngine {

  public GenericJavaServerEngine(String engineName) {
    super(engineName, Type.SERVER, NO_CAPABILITIES);
  }

  @Override
  public ExecutionResult execute(EnginePhysicalPlan plan, ErrorCollector errors) {
    //Executed at runtime
    return new ExecutionResult.Message("SUCCESS");
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs,
      ExecutionPipeline pipeline, RelBuilder relBuilder, TableSink errorSink) {
    Set<ExecutionStage> dbStages = pipeline.getStages().stream().filter(s -> s.getEngine().getType()==Type.DATABASE).collect(
        Collectors.toSet());
    Preconditions.checkArgument(dbStages.size()==1, "Currently only support a single database stage in server");
    ExecutionEngine engine = Iterables.getOnlyElement(dbStages).getEngine();
    Preconditions.checkArgument(engine instanceof JDBCEngine, "Currently the server only supports JDBC databases");
    return new ServerPhysicalPlan(plan.getModel(), ((JDBCEngine)engine).getConnector());
  }

  @Override
  public EnginePhysicalPlan readPlanFrom(Path directory, String stageName, Deserializer deserializer) throws IOException {
    return ServerPhysicalPlan.readFrom(directory, stageName, deserializer);
  }
}
