package com.datasqrl.engine.kafka;

import static com.datasqrl.io.tables.TableConfig.Base.TIMESTAMP_COL_KEY;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.log.Log;
import com.datasqrl.engine.log.LogEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.plan.global.PhysicalDAGPlan.LogStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.plan.table.RelDataTypeTableSchema;
import com.datasqrl.schema.TableSchemaExporterFactory;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.kafka.common.internals.Topic;

@Slf4j
public class KafkaLogEngine extends ExecutionEngine.Base implements LogEngine {

  public static final String DEFAULT_EVENT_TIME_NAME = "event_time";
  public static final String DEFAULT_EVENT_ID_NAME = "event_id";

  private final SqrlConfig connectorConfig;
  private final Optional<TableSchemaExporterFactory> schemaFactory;
  private final com.datasqrl.engine.kafka.KafkaConnectorFactory connectorFactory;

  public KafkaLogEngine(SqrlConfig connectorConfig, Optional<TableSchemaExporterFactory> schemaFactory,
      KafkaConnectorFactory connectorFactory) {
    super(KafkaLogEngineFactory.ENGINE_NAME, Type.LOG, EnumSet.noneOf(EngineFeature.class));
    this.connectorConfig = connectorConfig;
    this.schemaFactory = schemaFactory;
    this.connectorFactory = connectorFactory;
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs, ExecutionPipeline pipeline,
      SqrlFramework relBuilder, ErrorCollector errorCollector) {
    Preconditions.checkArgument(plan instanceof LogStagePlan);
    return new KafkaPhysicalPlan(this.connectorConfig,
        ((LogStagePlan) plan).getLogs().stream()
            .map(log -> (com.datasqrl.engine.kafka.KafkaTopic)log)
            .map(t->new NewTopic(t.getTopicName()))
            .collect(Collectors.toList()));
  }

  @Override
  public Log createLog(String logId, RelDataTypeField schema, List<String> primaryKey,
      Optional<String> timestamp) {
    String topicName = sanitizeName(logId);
    Preconditions.checkArgument(Topic.isValid(topicName), "Not a valid topic name: %s", topicName);

    Optional<TableSchema> tblSchema = Optional.of(new RelDataTypeTableSchema(schema.getType()));

    TableConfig.Builder tblBuilder = buildLog(Name.system(schema.getName()), connectorConfig, topicName);
    if (!primaryKey.isEmpty()) tblBuilder.setPrimaryKey(primaryKey.toArray(new String[0]));
    TableConfig logConfig = tblBuilder.build();
    timestamp.map(ts -> logConfig.getBaseTableConfig().getBaseConfig().setProperty(TIMESTAMP_COL_KEY, ts));
    NamePath path = Name.system(schema.getName()).toNamePath();
    TableSource tableSource = logConfig.initializeSource(path, tblSchema.orElse(null));
    return new KafkaTopic(topicName, tableSource,
        logConfig.initializeSink(path, tblSchema)
    );
  }

  private TableConfig.Builder buildLog(@NonNull Name name,
      @NonNull SqrlConfig connectorConfig, @NonNull String topic) {
    TableConfig.Builder builder = TableConfig.builder(name);
    builder.setType(ExternalDataType.source_and_sink);
    builder.setTimestampColumn(DEFAULT_EVENT_TIME_NAME);
    builder.setWatermark(0);
    builder.setMetadata(DEFAULT_EVENT_TIME_NAME, "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)", connectorFactory.getEventTime());
    builder.copyConnectorConfig(connectorFactory.fromBaseConfig(connectorConfig, topic));
    return builder;
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
