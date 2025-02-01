package com.datasqrl.engine.export;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineType;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.config.TableConfig;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.log.kafka.KafkaLogEngineFactory;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.google.inject.Inject;
import java.util.EnumSet;
import java.util.List;

public class PrintEngine implements ExportEngine {

  @Inject
  public PrintEngine(PackageJson json,
      ConnectorFactoryFactory connectorFactory) {
    this.engineConfig = json.getEngines().getEngineConfig(KafkaLogEngineFactory.ENGINE_NAME)
        .orElseGet(() -> new EmptyEngineConfig(KafkaLogEngineFactory.ENGINE_NAME));
    this.connectorFactory = connectorFactory.create(EngineType.LOG, "kafka")
        .orElseThrow(()->new RuntimeException("Could not find kafka connector"));
  }


  @Override
  public boolean supports(EngineFeature capability) {
    return false;
  }

  @Override
  public TableConfig getSinkConfig(String sinkName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs, ExecutionPipeline pipeline,
      List<StagePlan> stagePlans, SqrlFramework framework, ErrorCollector errorCollector) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getName() {
    return PrintEngineFactory.NAME;
  }

  @Override
  public EngineType getType() {
    return EngineType.EXPORT;
  }
}
