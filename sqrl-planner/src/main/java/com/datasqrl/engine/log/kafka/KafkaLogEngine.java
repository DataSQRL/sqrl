package com.datasqrl.engine.log.kafka;


import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.config.ConnectorFactory;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.log.LogEngine;
import com.datasqrl.engine.log.LogFactory;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.PhysicalDAGPlan.LogStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.schema.TableSchemaExporterFactory;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaLogEngine extends ExecutionEngine.Base implements LogEngine {

  @Getter
  private final EngineConfig engineConfig;
//  private final Optional<TableSchemaExporterFactory> schemaFactory;
  private final ConnectorFactory connectorFactory;

  @Inject
  public KafkaLogEngine(PackageJson json,
//      Optional<TableSchemaExporterFactory> schemaFactory,
      ConnectorFactoryFactory connectorFactory) {
    super(KafkaLogEngineFactory.ENGINE_NAME, Type.LOG, EnumSet.noneOf(EngineFeature.class));
    this.engineConfig = json.getEngines().getEngineConfig(KafkaLogEngineFactory.ENGINE_NAME)
        .orElseGet(()->new EmptyEngineConfig(KafkaLogEngineFactory.ENGINE_NAME));
//    this.schemaFactory = schemaFactory;
    this.connectorFactory = connectorFactory.create(Type.LOG, "kafka").orElse(null);
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs, ExecutionPipeline pipeline,
      SqrlFramework relBuilder, ErrorCollector errorCollector) {
    Preconditions.checkArgument(plan instanceof LogStagePlan);
    return new KafkaPhysicalPlan(
        ((LogStagePlan) plan).getLogs().stream()
            .map(log -> (KafkaTopic)log)
            .map(t-> new NewTopic(t.getTopicName(), 1, Short.parseShort("1"), Map.of(), Map.of()))
            .collect(Collectors.toList()));
  }

  @Override
  public LogFactory getLogFactory() {
    return new KafkaLogFactory(connectorFactory);
  }
}
