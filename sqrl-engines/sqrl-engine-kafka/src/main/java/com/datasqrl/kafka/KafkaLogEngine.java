package com.datasqrl.kafka;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.EngineCapability;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.log.Log;
import com.datasqrl.engine.log.LogEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.NotYetImplementedException;
import com.datasqrl.io.DataSystemDiscovery;
import com.datasqrl.io.DataSystemDiscovery.Base;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.LogStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.schema.TableSchemaExporterFactory;
import com.datasqrl.schema.UniversalTable;
import com.google.common.base.Preconditions;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.calcite.tools.RelBuilder;
import org.apache.kafka.common.internals.Topic;

public class KafkaLogEngine extends ExecutionEngine.Base implements LogEngine {

  private final TableConfig config;
  private final TableSchemaExporterFactory schemaFactory;

  public KafkaLogEngine(TableConfig config, TableSchemaExporterFactory schemaFactory) {
    super(KafkaLogEngineFactory.ENGINE_NAME, Type.LOG, EnumSet.noneOf(EngineCapability.class));
    this.config = config;
    this.schemaFactory = schemaFactory;
  }

  @Override
  public CompletableFuture<ExecutionResult> execute(EnginePhysicalPlan plan,
      ErrorCollector errors) {
    //TODO: connect to Kafka cluster and create topics
    throw new NotYetImplementedException("Topic creation not yet implemented");
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs, ExecutionPipeline pipeline,
      RelBuilder relBuilder, TableSink errorSink) {
    Preconditions.checkArgument(plan instanceof LogStagePlan);
    return new KafkaPhysicalPlan(
        ((LogStagePlan) plan).getLogs().stream()
            .map(log -> (KafkaTopic)log).collect(Collectors.toList()));
  }

  @Override
  public Log createLog(String logId, UniversalTable schema) {
    String topicName = sanitizeName(logId);
    Preconditions.checkArgument(Topic.isValid(topicName), "Not a valid topic name: %s", topicName);
    TableSchema tblSchema = schemaFactory.convert(schema);
    TableConfig.Builder tblBuilder = DataSystemDiscovery.Base.copyGeneric(config, schema.getName(), topicName, ExternalDataType.source_and_sink);
    TableConfig logConfig = tblBuilder.build();
    NamePath path = schema.getName().toNamePath();
    return new KafkaTopic(topicName,
        logConfig.initializeSource(path, tblSchema),
        logConfig.initializeSink(path, Optional.of(tblSchema)));
  }

  static String sanitizeName(String logId) {
    String sanitizedName = logId;
    for (char invalidChar : REPLACE_CHARS) {
      sanitizedName = sanitizedName.replace(invalidChar,REPLACE_WITH);
    }
    return sanitizedName;
  }

  public static final char[] REPLACE_CHARS = {'$'};
  public static final char REPLACE_WITH = '-';

}
