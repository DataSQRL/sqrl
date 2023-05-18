package com.datasqrl.engine.log;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineCapability;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionEngine.Type;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.google.auto.service.AutoService;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.tools.RelBuilder;

@Slf4j
@AutoService(EngineFactory.class)
public class KafkaLogEngineFactory implements EngineFactory {

  public static final String ENGINE_NAME = "kafka";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public Type getEngineType() {
    return Type.LOG;
  }

  @Override
  public ExecutionEngine initialize(@NonNull SqrlConfig config) {
    return new KafkaLogEngine(config);
  }

  public static class KafkaLogEngine extends ExecutionEngine.Base implements ExecutionEngine {

    private final SqrlConfig config;

    public KafkaLogEngine(@NonNull SqrlConfig config) {
      super(ENGINE_NAME, Type.LOG, EngineCapability.NO_CAPABILITIES);
      this.config = config;
    }

    @Override
    public CompletableFuture<ExecutionResult> execute(EnginePhysicalPlan plan,
        ErrorCollector errors) {
      //Start up in-mem kafka instance, run init script

      return null;
    }

    @Override
    public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs,
        ExecutionPipeline pipeline, RelBuilder relBuilder, TableSink errorSink) {

      return null;
    }

    @Override
    public void generateAssets(Path buildDir) {
      //Need to get the relevant topics to create an init script
    }
  }
}
