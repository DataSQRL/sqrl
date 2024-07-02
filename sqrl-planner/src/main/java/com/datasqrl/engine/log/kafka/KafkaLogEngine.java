package com.datasqrl.engine.log.kafka;


import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.config.ConnectorFactory;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineFactory.Type;
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
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.ExternalSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.LogStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
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
        .orElseGet(() -> new EmptyEngineConfig(KafkaLogEngineFactory.ENGINE_NAME));
//    this.schemaFactory = schemaFactory;
    this.connectorFactory = connectorFactory.create(Type.LOG, "kafka").orElse(null);
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs,
      List<EngineSink> engineSinks, ExecutionPipeline pipeline,
      SqrlFramework framework, ErrorCollector errorCollector) {

    Preconditions.checkArgument(plan instanceof LogStagePlan);

    List<NewTopic> topics = new ArrayList<>();

    ((LogStagePlan) plan).getLogs().stream()
        .map(log -> (KafkaTopic) log)
        .map(KafkaTopic::getTopicName)
        .map(NewTopic::new)
        .forEach(topics::add);

//    engineSinks.stream()
//        .map(EngineSink::getName)
//        .map(NewTopic::new)
//        .forEach(topics::add);

//    List<ResolvedExport> exports = framework.getSchema().getExports();
//    exports.stream()
//        .flatMap(resolvedExport -> engineSinks.stream()
//            .map(EngineSink::getTableSink)
//            .filter(externalTableSink -> externalTableSink.equals(resolvedExport.getSink()))
//            .findAny().stream())
//        .map(externalTableSink -> externalTableSink.getName().getCanonical())
//        .map(NewTopic::new)
//        .forEach(topics::add);

    return new KafkaPhysicalPlan(topics);
  }

  @Override
  public LogFactory getLogFactory() {
    return new KafkaLogFactory(connectorFactory);
  }
}
